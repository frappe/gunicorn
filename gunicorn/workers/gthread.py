#
# This file is part of gunicorn released under the MIT license.
# See the NOTICE for more information.

# design:
# A threaded worker accepts connections in the main loop, accepted
# connections are added to the thread pool as a connection job.
# Keepalive connections are put back in the loop waiting for an event.
# If no event happen after the keep alive timeout, the connection is
# closed.
# pylint: disable=no-else-break

from concurrent import futures
import errno
import faulthandler
import os
import queue
import selectors
import socket
import ssl
import sys
import time
from collections import deque
from datetime import datetime
from functools import partial

from . import base
from .gthread_routing import SlowRoutePredictor
from .. import http
from .. import util
from .. import sock
from ..http import wsgi


# Sentinel value to indicate connection should be deferred back to poller
_DEFER = object()

# Default timeout (in seconds) for waiting for request data in worker thread.
# If no data arrives within this timeout, the connection is deferred back to
# the main poller to prevent thread pool exhaustion from slow clients.
DEFAULT_WORKER_DATA_TIMEOUT = 5.0

# how many bytes to peek when classifying a request by its request line
REQUEST_LINE_PEEK = 8192


class TConn:

    def __init__(self, cfg, sock, client, server):
        self.cfg = cfg
        self.sock = sock
        self.client = client
        self.server = server

        self.timeout = None
        self.parser = None
        self.initialized = False
        self.is_http2 = False
        # Track if we've already waited for data (to avoid waiting again after defer)
        self.data_ready = False
        # route key (method + path), set by the worker when request routing is
        # enabled; used to predict and learn slow routes
        self.route_key = None
        # set by ``handle`` when a pool thread starts processing the request;
        # used by the run loop to enforce a per-request timeout and to learn
        # slow routes. Stays None while the request is queued or parked so the
        # timeout only covers actual processing.
        self.exec_start_time = None

        # set the socket to non blocking
        self.sock.setblocking(False)

    def init(self):
        # Guard against double initialization
        if self.initialized:
            return
        self.initialized = True
        self.sock.setblocking(True)

        if self.parser is None:
            # wrap the socket if needed
            if self.cfg.is_ssl:
                self.sock = sock.ssl_wrap_socket(self.sock, self.cfg)

                # Complete the handshake to ensure ALPN negotiation is done
                # (needed if do_handshake_on_connect is False)
                if not self.cfg.do_handshake_on_connect:
                    self.sock.do_handshake()

                # Check if HTTP/2 was negotiated via ALPN
                if sock.is_http2_negotiated(self.sock):
                    self.is_http2 = True
                    self.parser = http.get_parser(
                        self.cfg, self.sock, self.client, http2_connection=True
                    )
                    self.parser.initiate_connection()
                    return

            # initialize the HTTP/1.x parser
            self.parser = http.get_parser(self.cfg, self.sock, self.client)

    def set_timeout(self):
        # Use monotonic clock for reliability (time.time() can jump due to NTP)
        self.timeout = time.monotonic() + self.cfg.keepalive

    def wait_for_data(self, timeout):
        """Wait for data to be available on the socket.

        Uses selectors to wait for the socket to become readable within
        the given timeout. This prevents slow clients from blocking
        thread pool slots indefinitely.

        Args:
            timeout: Maximum time to wait in seconds.

        Returns:
            True if data is available, False if timeout expired.
        """
        if self.data_ready:
            return True

        # Use a temporary selector to wait for data
        sel = selectors.DefaultSelector()
        try:
            sel.register(self.sock, selectors.EVENT_READ)
            events = sel.select(timeout=timeout)
            if events:
                self.data_ready = True
                return True
            return False
        except (OSError, ValueError):
            # Socket closed or invalid
            return False
        finally:
            sel.close()

    def close(self, graceful=False):
        if graceful:
            self.sock.setblocking(True)
            util.close_graceful(self.sock)
        else:
            util.close(self.sock)


class PollableMethodQueue:
    """Thread-safe queue that can wake up a selector.

    Uses a pipe to allow worker threads to signal the main thread
    when work is ready, enabling lock-free coordination.

    This approach is compatible with all POSIX systems including
    Linux, macOS, FreeBSD, OpenBSD, and NetBSD. The pipe is set to
    non-blocking mode to prevent worker threads from blocking if
    the pipe buffer fills up under extreme load.
    """

    def __init__(self):
        self._read_fd = None
        self._write_fd = None
        self._queue = None

    def init(self):
        """Initialize the pipe and queue."""
        self._read_fd, self._write_fd = os.pipe()
        # Set both ends to non-blocking:
        # - Write: prevents worker threads from blocking if buffer is full
        # - Read: allows run_callbacks to drain without blocking
        os.set_blocking(self._read_fd, False)
        os.set_blocking(self._write_fd, False)
        self._queue = queue.SimpleQueue()

    def close(self):
        """Close the pipe file descriptors."""
        if self._read_fd is not None:
            try:
                os.close(self._read_fd)
            except OSError:
                pass
        if self._write_fd is not None:
            try:
                os.close(self._write_fd)
            except OSError:
                pass

    def fileno(self):
        """Return the readable file descriptor for selector registration."""
        return self._read_fd

    def defer(self, callback, *args):
        """Queue a callback to be run on the main thread.

        The callback is added to the queue first, then a wake-up byte
        is written to the pipe. If the pipe write fails (buffer full),
        it's safe to ignore because the main thread will eventually
        drain the queue when it reads other wake-up bytes.
        """
        self._queue.put(partial(callback, *args))
        try:
            os.write(self._write_fd, b'\x00')
        except OSError:
            # Pipe buffer full (EAGAIN/EWOULDBLOCK) - safe to ignore
            # The main thread will still process the queue
            pass

    def run_callbacks(self, _fileobj, max_callbacks=50):
        """Run queued callbacks. Called when the pipe is readable.

        Drains all available wake-up bytes and runs corresponding callbacks.
        The max_callbacks limit prevents starvation of other event sources.
        """
        # Read all available wake-up bytes (up to limit)
        try:
            data = os.read(self._read_fd, max_callbacks)
        except OSError:
            return

        # Run callbacks for each byte read, plus any extras in queue
        # (extras can accumulate if pipe writes were dropped)
        callbacks_run = 0
        while callbacks_run < len(data) + 10:  # +10 to drain dropped writes
            try:
                callback = self._queue.get_nowait()
                callback()
                callbacks_run += 1
            except queue.Empty:
                break


class ThreadWorker(base.Worker):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.worker_connections = self.cfg.worker_connections
        self.max_keepalived = self.cfg.worker_connections - self.cfg.threads

        # adaptive queueing: when enabled, the configured threads are split
        # into a fast lane (``self.tpool``) and a slow lane (``self.slow_pool``)
        # so slow requests cannot starve fast ones
        self.routing_enabled = (
            self.cfg.enable_adaptive_queueing and self.cfg.threads >= 2)
        self.slow_threshold = self.cfg.slow_request_threshold

        self.tpool = None
        self.slow_pool = None
        self.poller = None
        self.method_queue = PollableMethodQueue()
        self.keepalived_conns = deque()
        # Connections waiting for data (deferred from thread pool) and
        # connections parked for request-line classification when routing is
        # enabled. Both are reaped by murder_pending via their timeout.
        self.pending_conns = deque()
        # in-flight request futures, tracked so the run loop can enforce a
        # per-request timeout (gthread has none upstream) and learn slow routes
        self.futures = set()
        self.nr_conns = 0
        self._accepting = False
        self.predictor = None

    @classmethod
    def check_config(cls, cfg, log):
        max_keepalived = cfg.worker_connections - cfg.threads

        if max_keepalived <= 0 and cfg.keepalive:
            log.warning("No keepalived connections can be handled. " +
                        "Check the number of worker connections and threads.")

        if cfg.enable_adaptive_queueing and cfg.threads < 2:
            log.warning("enable_adaptive_queueing requires at least 2 threads; "
                        "running with a single pool.")

    def init_process(self):
        if self.routing_enabled:
            # split the configured threads roughly evenly between the two
            # lanes; the fast lane gets the extra thread when threads is odd
            slow = self.cfg.threads // 2
            fast = self.cfg.threads - slow
            self.tpool = futures.ThreadPoolExecutor(max_workers=fast)
            self.slow_pool = futures.ThreadPoolExecutor(max_workers=slow)
            self.predictor = SlowRoutePredictor(self.slow_threshold)
            self.log.debug("adaptive queueing enabled: fast=%d slow=%d "
                           "threshold=%.1fs", fast, slow, self.slow_threshold)
        else:
            self.tpool = self.get_thread_pool()
        self.poller = selectors.DefaultSelector()
        self.method_queue.init()
        super().init_process()

    def get_thread_pool(self):
        """Override this method to customize how the thread pool is created"""
        return futures.ThreadPoolExecutor(max_workers=self.cfg.threads)

    def _shutdown_pools(self, wait):
        for pool in (self.tpool, self.slow_pool):
            if pool is not None:
                pool.shutdown(wait=wait)

    def handle_exit(self, sig, frame):
        """Handle SIGTERM - begin graceful shutdown."""
        if self.alive:
            self.alive = False
            # Wake up the poller so it can start shutdown
            self.method_queue.defer(lambda: None)

    def handle_quit(self, sig, frame):
        """Handle SIGQUIT - immediate shutdown."""
        self._shutdown_pools(wait=False)
        super().handle_quit(sig, frame)

    def set_accept_enabled(self, enabled):
        """Enable or disable accepting new connections."""
        if enabled == self._accepting:
            return

        for listener in self.sockets:
            if enabled:
                listener.setblocking(False)
                self.poller.register(listener, selectors.EVENT_READ, self.accept)
            else:
                self.poller.unregister(listener)

        self._accepting = enabled

    def enqueue_req(self, conn, slow=False):
        """Submit connection to a thread pool for processing."""
        # reset the per-request clock; ``handle`` sets it once the request is
        # actually being processed, so queue/keepalive idle time is not counted
        # toward the request timeout (and a stale value cannot trigger a kill).
        conn.exec_start_time = None
        if self.routing_enabled and slow:
            fs = self.slow_pool.submit(self.handle, conn)
        else:
            fs = self.tpool.submit(self.handle, conn)
        fs.conn = conn
        fs.slow = slow
        fs._observed_slow = False
        self.futures.add(fs)
        fs.add_done_callback(
            lambda fut: self.method_queue.defer(self.finish_request, conn, fut))

    def accept(self, listener):
        """Accept a new connection from a listener socket."""
        try:
            client_sock, client_addr = listener.accept()
            self.nr_conns += 1
            client_sock.setblocking(True)

            conn = TConn(self.cfg, client_sock, client_addr, listener.getsockname())

            if self.routing_enabled and not self.cfg.is_ssl:
                # park until the request line is readable, then classify the
                # lane. SSL is excluded: the request line cannot be peeked
                # before the TLS handshake.
                self.park_for_request(conn)
            else:
                # Submit directly to thread pool for processing
                self.enqueue_req(conn)
        except OSError as e:
            if e.errno not in (errno.EAGAIN, errno.ECONNABORTED, errno.EWOULDBLOCK):
                raise

    def park_for_request(self, conn):
        """Register a connection in the poller until its request line arrives."""
        conn.sock.setblocking(False)
        conn.set_timeout()
        self.pending_conns.append(conn)
        self.poller.register(conn.sock, selectors.EVENT_READ,
                             partial(self.classify_and_dispatch, conn))

    def classify_and_dispatch(self, conn, client=None):
        """Peek the request line, predict the lane, and enqueue the request."""
        line, closed, complete = self._peek_request_line(conn)
        if not closed and not complete:
            # request line has not fully arrived yet; keep waiting. Stalled
            # clients are reaped by murder_pending via the connection timeout.
            return

        try:
            # remove the connection from the parked set
            self.pending_conns.remove(conn)
        except ValueError:
            # already handled (e.g. by murder_pending); nothing to do
            return
        try:
            self.poller.unregister(conn.sock)
        except (KeyError, OSError, ValueError):
            pass

        if closed:
            self.nr_conns -= 1
            conn.close()
            return

        conn.route_key = self._route_key(line)
        # the request line has fully arrived; the worker thread need not wait
        # for data again before processing
        conn.data_ready = True
        slow = self.predictor.is_slow(conn.route_key)
        self.log.debug("routing %r to %s lane", conn.route_key,
                       "slow" if slow else "fast")
        self.enqueue_req(conn, slow=slow)

    def _peek_request_line(self, conn):
        """Return ``(line, closed, complete)`` for the connection's request line.

        ``line`` is the request line bytes (without CRLF) once available,
        ``closed`` is True if the peer closed the connection, and ``complete``
        is True once we should stop waiting for more data.
        """
        try:
            data = conn.sock.recv(REQUEST_LINE_PEEK, socket.MSG_PEEK)
        except (BlockingIOError, InterruptedError):
            return None, False, False
        except OSError:
            return None, True, False

        if data == b"":
            # peer closed the connection before sending a request
            return None, True, False

        idx = data.find(b"\r\n")
        if idx == -1:
            if len(data) >= REQUEST_LINE_PEEK:
                # request line longer than our peek window; stop classifying and
                # let the worker's parser deal with (or reject) it
                return None, False, True
            return None, False, False
        return data[:idx], False, True

    @staticmethod
    def _route_key(line):
        """Build a route key (``"METHOD /path"``) from a raw request line."""
        if not line:
            return None
        parts = line.split(b" ")
        if len(parts) < 2:
            return None
        try:
            method = parts[0].decode("latin1")
            path = parts[1].split(b"?", 1)[0].decode("latin1")
        except UnicodeDecodeError:
            return None
        return method + " " + path

    def on_client_socket_readable(self, conn, client):
        """Handle a keepalive connection becoming readable."""
        self.poller.unregister(client)
        self.keepalived_conns.remove(conn)

        # Submit to thread pool for processing
        self.enqueue_req(conn)

    def on_pending_socket_readable(self, conn, client):
        """Handle a pending (deferred) connection becoming readable."""
        self.poller.unregister(client)
        self.pending_conns.remove(conn)

        # Mark data as ready so we don't wait again in handle()
        conn.data_ready = True

        # Submit to thread pool for processing
        self.enqueue_req(conn)

    def murder_keepalived(self):
        """Close expired keepalive connections."""
        now = time.monotonic()
        while self.keepalived_conns:
            conn = self.keepalived_conns[0]
            delta = conn.timeout - now
            if delta > 0:
                break

            # Connection has timed out
            self.keepalived_conns.popleft()
            try:
                self.poller.unregister(conn.sock)
            except (OSError, KeyError, ValueError):
                pass  # Already unregistered
            self.nr_conns -= 1
            conn.close()

    def murder_pending(self):
        """Close expired pending connections (waiting for initial data)."""
        now = time.monotonic()
        while self.pending_conns:
            conn = self.pending_conns[0]
            delta = conn.timeout - now
            if delta > 0:
                break

            # Connection has timed out waiting for data
            self.pending_conns.popleft()
            try:
                self.poller.unregister(conn.sock)
            except (OSError, KeyError, ValueError):
                pass  # Already unregistered
            self.nr_conns -= 1
            conn.close()

    def is_parent_alive(self):
        # If our parent changed then we shut down.
        if self.ppid != os.getppid():
            self.log.info("Parent changed, shutting down: %s", self)
            return False
        return True

    def wait_for_and_dispatch_events(self, timeout):
        """Wait for events and dispatch callbacks."""
        try:
            events = self.poller.select(timeout)
            for key, _ in events:
                callback = key.data
                callback(key.fileobj)
        except OSError as e:
            if e.errno != errno.EINTR:
                raise

    def enforce_request_timeout(self):
        """Kill the worker if any in-flight request exceeds the timeout.

        gthread has no built-in request timeout; once a request is handed to a
        worker thread there is no safe way to interrupt it, so the only robust
        option is to take the whole worker down (the arbiter then replaces it).
        While taking the worker down a traceback is dumped to aid debugging.

        The same pass proactively learns slow routes: an in-flight fast-lane
        request that crosses the slow threshold marks its route slow so the
        rest of a burst is rerouted without waiting for it to finish.
        """
        now = time.monotonic()
        for fut in list(self.futures):
            # ``exec_start_time`` is None while a request is still waiting for
            # client data (upstream's _DEFER path) or queued, and stays None for
            # long-lived HTTP/2 connections, so those are never timed out here.
            if fut.done() or fut.conn.exec_start_time is None:
                continue
            elapsed = now - fut.conn.exec_start_time
            if self.cfg.timeout and elapsed > self.cfg.timeout:
                self.alive = False
                self.log.error("A request timed out. Exiting.")
                faulthandler.dump_traceback()
            elif (self.routing_enabled and not fut._observed_slow
                    and not fut.slow
                    and elapsed > self.slow_threshold):
                self.predictor.observe_slow(fut.conn.route_key)
                fut._observed_slow = True
                self.log.debug("in-flight request %r crossed threshold; "
                               "marking route slow", fut.conn.route_key)

    def run(self):
        # Register the method queue with the poller
        self.poller.register(self.method_queue.fileno(),
                             selectors.EVENT_READ,
                             self.method_queue.run_callbacks)

        # Start accepting connections
        self.set_accept_enabled(True)

        while self.alive:
            # Notify the arbiter we are alive
            self.notify()

            # Check if we can accept more connections
            can_accept = self.nr_conns < self.worker_connections
            if can_accept != self._accepting:
                self.set_accept_enabled(can_accept)

            # Wait for events (unified event loop - no futures.wait())
            self.wait_for_and_dispatch_events(timeout=1.0)

            if not self.is_parent_alive():
                break

            # Handle keepalive and pending connection timeouts
            self.murder_keepalived()
            self.murder_pending()

            # Enforce the per-request timeout and learn slow routes
            self.enforce_request_timeout()

        # Graceful shutdown: stop accepting but handle existing connections
        self.set_accept_enabled(False)

        # Wait for in-flight connections within grace period
        graceful_timeout = time.monotonic() + self.cfg.graceful_timeout
        while self.nr_conns > 0:
            time_remaining = max(graceful_timeout - time.monotonic(), 0)
            if time_remaining == 0:
                break
            self.wait_for_and_dispatch_events(timeout=time_remaining)
            self.murder_keepalived()
            self.murder_pending()

        # Cleanup
        self._shutdown_pools(wait=False)
        self.poller.close()
        self.method_queue.close()

        for s in self.sockets:
            s.close()

    def finish_request(self, conn, fs):
        """Handle completion of a request (called via method_queue on main thread)."""
        # stop tracking this future for request-timeout purposes
        self.futures.discard(fs)

        # feed the observed processing time back to the predictor so the route
        # is learned (or unlearned) as slow
        if (self.routing_enabled and conn.route_key
                and conn.exec_start_time is not None):
            duration = time.monotonic() - conn.exec_start_time
            self.predictor.update(conn.route_key, duration)
            self.log.debug("observed %r took %.3fs", conn.route_key, duration)

        try:
            result = fs.result() if not fs.cancelled() else False

            if result is _DEFER and self.alive:
                # Connection deferred - no data arrived within timeout.
                # Put it on the poller to wait for data without consuming a thread.
                conn.sock.setblocking(False)
                # Use keepalive timeout for pending connections too
                conn.timeout = time.monotonic() + self.cfg.keepalive
                self.pending_conns.append(conn)
                self.poller.register(conn.sock, selectors.EVENT_READ,
                                     partial(self.on_pending_socket_readable, conn))
            elif result and self.alive:
                if self.routing_enabled and not self.cfg.is_ssl:
                    # re-classify the next request on this keepalive connection
                    self.park_for_request(conn)
                else:
                    # Keepalive - put connection back in the poller
                    conn.sock.setblocking(False)
                    conn.set_timeout()
                    self.keepalived_conns.append(conn)
                    self.poller.register(conn.sock, selectors.EVENT_READ,
                                         partial(self.on_client_socket_readable, conn))
            else:
                self.nr_conns -= 1
                conn.close(graceful=True)
        except Exception:
            self.nr_conns -= 1
            conn.close()

    def handle(self, conn):
        """Handle a request on a connection. Runs in a worker thread."""
        req = None
        try:
            # For new connections (not yet initialized), wait for data with timeout
            # to prevent slow clients from blocking thread pool slots indefinitely.
            # Skip this for already-initialized connections (keepalive, deferred)
            # since they're coming from the poller and data is already available.
            if not conn.initialized and not conn.data_ready:
                # Wait for data with timeout before committing this thread
                if not conn.wait_for_data(DEFAULT_WORKER_DATA_TIMEOUT):
                    # No data within timeout - defer to poller
                    return _DEFER

            # Always ensure blocking mode in worker thread.
            # Critical for keepalive connections: the socket is set to non-blocking
            # for the selector in finish_request(), but must be blocking for
            # request/body reading to avoid SSLWantReadError on SSL connections.
            conn.sock.setblocking(True)

            # Initialize connection in worker thread to handle SSL errors gracefully
            # (ENOTCONN from ssl_wrap_socket would crash main thread otherwise)
            conn.init()

            # HTTP/2 connections require special handling
            if conn.is_http2:
                # HTTP/2 connections are long-lived and multiplex many streams,
                # so they manage their own lifecycle and are exempt from the
                # per-request timeout (exec_start_time stays None).
                return self.handle_http2(conn)

            # We are now committed to processing this HTTP/1 request: start the
            # clock used by the worker-level request timeout and slow-route
            # learning. Client wait time and SSL setup are deliberately excluded.
            conn.exec_start_time = time.monotonic()

            req = next(conn.parser)
            if not req:
                return False

            # Handle the request
            keepalive = self.handle_request(req, conn)
            if keepalive:
                # Discard any unread request body before keepalive to prevent
                # the socket from appearing readable due to leftover bytes.
                # Bound the drain by the worker data timeout: a stalled client
                # must not keep this thread blocked.
                drain_deadline = time.monotonic() + DEFAULT_WORKER_DATA_TIMEOUT
                if not conn.parser.finish_body(deadline=drain_deadline):
                    # Abandon keepalive when the body could not be fully drained.
                    return False
                return True
        except http.errors.NoMoreData as e:
            self.log.debug("Ignored premature client disconnection. %s", e)
        except StopIteration as e:
            self.log.debug("Closing connection. %s", e)
        except ssl.SSLError as e:
            if e.args[0] == ssl.SSL_ERROR_EOF:
                self.log.debug("ssl connection closed")
                conn.sock.close()
            else:
                self.log.debug("Error processing SSL request.")
                self.handle_error(req, conn.sock, conn.client, e)
        except OSError as e:
            if e.errno not in (errno.EPIPE, errno.ECONNRESET, errno.ENOTCONN):
                self.log.exception("Socket error processing request.")
            else:
                if e.errno == errno.ECONNRESET:
                    self.log.debug("Ignoring connection reset")
                elif e.errno == errno.ENOTCONN:
                    self.log.debug("Ignoring socket not connected")
                else:
                    self.log.debug("Ignoring connection epipe")
        except Exception as e:
            self.handle_error(req, conn.sock, conn.client, e)

        return False

    def handle_http2(self, conn):
        """Handle an HTTP/2 connection. Runs in a worker thread.

        HTTP/2 connections are persistent and multiplex multiple streams.
        We handle all streams until the connection is closed.

        Returns:
            False (HTTP/2 connections don't use keepalive polling)
        """
        h2_conn = conn.parser  # HTTP2ServerConnection

        try:
            while not h2_conn.is_closed and self.alive:
                # Receive data and get completed requests
                requests = h2_conn.receive_data()

                for req in requests:
                    try:
                        self.handle_http2_request(req, conn, h2_conn)
                    except Exception as e:
                        self.log.exception("Error handling HTTP/2 request")
                        try:
                            h2_conn.send_error(req.stream.stream_id, 500, str(e))
                        except Exception:
                            pass
                    finally:
                        # Cleanup stream after processing
                        h2_conn.cleanup_stream(req.stream.stream_id)

                # Check if we need to close
                if not self.alive:
                    h2_conn.close()
                    break

        except http.errors.NoMoreData:
            self.log.debug("HTTP/2 connection closed by client")
        except ssl.SSLError as e:
            if e.args[0] == ssl.SSL_ERROR_EOF:
                self.log.debug("HTTP/2 SSL connection closed")
            else:
                self.log.debug("HTTP/2 SSL error: %s", e)
        except OSError as e:
            if e.errno not in (errno.EPIPE, errno.ECONNRESET, errno.ENOTCONN):
                self.log.exception("HTTP/2 socket error")
        except Exception:
            self.log.exception("HTTP/2 connection error")

        return False

    def handle_http2_request(self, req, conn, h2_conn):
        """Handle a single HTTP/2 request/stream."""
        environ = {}
        resp = None
        stream_id = req.stream.stream_id

        try:
            self.cfg.pre_request(self, req)
            request_start = datetime.now()

            # Create WSGI environ
            resp, environ = wsgi.create(req, conn.sock, conn.client,
                                        conn.server, self.cfg)
            environ["wsgi.multithread"] = True
            environ["HTTP_VERSION"] = "2"  # Indicate HTTP/2

            # Replace wsgi.early_hints with HTTP/2-specific version
            def send_early_hints_h2(headers):
                """Send 103 Early Hints over HTTP/2."""
                h2_conn.send_informational(stream_id, 103, headers)

            environ["wsgi.early_hints"] = send_early_hints_h2

            # Add HTTP/2 trailer support
            pending_trailers = []

            def send_trailers_h2(trailers):
                """Queue trailers to be sent after response body."""
                pending_trailers.extend(trailers)

            environ["gunicorn.http2.send_trailers"] = send_trailers_h2

            self.nr += 1
            if self.nr >= self.max_requests:
                if self.alive:
                    self.log.info("Autorestarting worker after current request.")
                    self.alive = False

            # Run WSGI app
            respiter = self.wsgi(environ, resp.start_response)

            # Collect response body
            response_body = b''
            try:
                if hasattr(respiter, '__iter__'):
                    for item in respiter:
                        if item:
                            response_body += item
            finally:
                if hasattr(respiter, "close"):
                    respiter.close()

            # Send response via HTTP/2
            if pending_trailers:
                # Send headers, body, then trailers separately
                # Build response headers with :status pseudo-header
                response_headers = [(':status', str(resp.status_code))]
                for name, value in resp.headers:
                    response_headers.append((name.lower(), str(value)))

                # Send headers without ending stream
                h2_conn.h2_conn.send_headers(stream_id, response_headers, end_stream=False)
                stream = h2_conn.streams[stream_id]
                stream.send_headers(response_headers, end_stream=False)
                h2_conn._send_pending_data()

                # Send body without ending stream
                if response_body:
                    h2_conn.h2_conn.send_data(stream_id, response_body, end_stream=False)
                    stream.send_data(response_body, end_stream=False)
                    h2_conn._send_pending_data()

                # Send trailers (ends stream)
                h2_conn.send_trailers(stream_id, pending_trailers)
            else:
                # No trailers, use standard response
                h2_conn.send_response(
                    stream_id,
                    resp.status_code,
                    resp.headers,
                    response_body
                )

            request_time = datetime.now() - request_start
            self.log.access(resp, req, environ, request_time)

        finally:
            try:
                self.cfg.post_request(self, req, environ, resp)
            except Exception:
                self.log.exception("Exception in post_request hook")

    def handle_request(self, req, conn):
        environ = {}
        resp = None
        try:
            self.cfg.pre_request(self, req)
            request_start = datetime.now()
            resp, environ = wsgi.create(req, conn.sock, conn.client,
                                        conn.server, self.cfg)
            environ["wsgi.multithread"] = True
            self.nr += 1
            if self.nr >= self.max_requests:
                if self.alive:
                    self.log.info("Autorestarting worker after current request.")
                    self.alive = False
                resp.force_close()

            if not self.alive or not self.cfg.keepalive:
                resp.force_close()
            elif len(self.keepalived_conns) >= self.max_keepalived:
                resp.force_close()

            respiter = self.wsgi(environ, resp.start_response)
            try:
                if isinstance(respiter, environ['wsgi.file_wrapper']):
                    resp.write_file(respiter)
                else:
                    for item in respiter:
                        resp.write(item)

                resp.close()
            finally:
                request_time = datetime.now() - request_start
                self.log.access(resp, req, environ, request_time)
                if hasattr(respiter, "close"):
                    respiter.close()

            if resp.should_close():
                self.log.debug("Closing connection.")
                return False
        except OSError:
            # pass to next try-except level
            util.reraise(*sys.exc_info())
        except Exception:
            if resp and resp.headers_sent:
                # If the requests have already been sent, we should close the
                # connection to indicate the error.
                self.log.exception("Error handling request")
                util.close_graceful(conn.sock)
                raise StopIteration()
            raise
        finally:
            try:
                self.cfg.post_request(self, req, environ, resp)
            except Exception:
                self.log.exception("Exception in post_request hook")

        return True
