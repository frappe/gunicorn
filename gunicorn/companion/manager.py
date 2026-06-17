#
# This file is part of gunicorn released under the MIT license.
# See the NOTICE for more information.


from __future__ import annotations

import ctypes
import importlib
import os
import select
import signal
import sys
import time
from typing import TYPE_CHECKING, Callable, Iterable, Union

from gunicorn import util
from gunicorn.companion.control import CommandError
from gunicorn.companion.process import CompanionProcess, State

if TYPE_CHECKING:
    from gunicorn.companion.config import CompanionConfig

# prctl option number for "send me this signal when my parent dies".
PR_SET_PDEATHSIG = 1


# Signals the arbiter and manager install handlers for; a forked companion
# resets them to the default so its stop signal works and the target starts
# from a clean slate, the same way a gunicorn worker does.
INHERITED_SIGNALS = [
    "SIGCHLD", "SIGTERM", "SIGINT", "SIGHUP", "SIGQUIT",
    "SIGUSR1", "SIGUSR2", "SIGWINCH", "SIGTTIN", "SIGTTOU",
]


def reset_child_signals() -> None:
    """Restore default signal handling in a freshly forked companion.

    The companion inherits the manager's (and arbiter's) handlers across the
    fork. Without this, a stop signal like SIGTERM would hit the manager's
    handler -- which just flips a flag -- instead of terminating the companion.
    """
    for name in INHERITED_SIGNALS:
        number = getattr(signal, name, None)
        if number is not None:
            signal.signal(number, signal.SIG_DFL)


def set_parent_death_signal(stop_signal) -> bool:
    """Ask the kernel to send ``stop_signal`` when this process's parent dies.

    Uses Linux ``prctl(PR_SET_PDEATHSIG)`` so an orphaned manager or companion
    is signalled the moment its parent goes away, rather than lingering. Returns
    True when armed and False on any non-Linux platform or error, so callers can
    fall back to polling ``os.getppid()``.
    """
    if not sys.platform.startswith("linux"):
        return False
    try:
        libc = ctypes.CDLL("libc.so.6", use_errno=True)
        return libc.prctl(PR_SET_PDEATHSIG, int(stop_signal), 0, 0, 0) == 0
    except (OSError, AttributeError):
        return False


class CompanionManager:
    """Forks and supervises companion processes.

    Created by the arbiter after preload. Holds one ``CompanionProcess`` per
    configured companion and owns the fork lifecycle. This skeleton wires
    construction and single-companion spawn; reaping, backoff, the control
    socket, and the run loop arrive in later tasks.
    """

    def __init__(self, configs: Iterable[CompanionConfig], log):
        self.log = log
        self.pid = os.getpid()
        self.processes = {c.name: CompanionProcess(c) for c in configs}
        # Set by the arbiter wiring: a no-arg callable that re-reads and
        # validates companion config, returning a fresh CompanionConfig list.
        self.config_loader = None
        # Set by the arbiter wiring: the ControlServer, or None when no control
        # socket is configured. Created inside the child by run().
        self.control = None
        self.stopping = False
        self._wakeup_pipe = None
        self.parent_pid = None

    def run(self) -> None:
        """Run the manager's supervision loop. This is the forked child body.

        Installs signal handling, brings up the control socket, starts every
        companion, then loops servicing the socket and the companions until a
        SIGTERM or SIGINT asks it to stop, at which point it shuts the
        companions down and returns. Each tick reaps exited companions,
        retries any that are backing off, promotes those past ``startsecs``,
        and kills any that overran their stop deadline.

        If the arbiter dies, the manager stops too: it arms a parent-death
        signal on Linux and, as a portable fallback, watches ``getppid`` each
        tick so it never keeps companions running under a dead arbiter.
        """
        # __init__ ran in the arbiter, so refresh pid/parent now that this is
        # the manager process: the companion parent-death guard compares its
        # getppid() against self.pid.
        self.pid = os.getpid()
        self.parent_pid = os.getppid()
        self._install_signals()
        set_parent_death_signal(signal.SIGTERM)
        if self.control is not None:
            self.control.create()
        for process in self.processes.values():
            self.spawn_process(process)
        self.log.info("companion manager running (pid %s)", self.pid)
        try:
            while not self.stopping:
                if self._parent_gone():
                    self.log.info("companion manager parent gone, stopping")
                    break
                self._tick()
                self._wait()
            self.stop_all()
        finally:
            if self.control is not None:
                self.control.close()
            self.log.info("companion manager stopped (pid %s)", self.pid)

    def _parent_gone(self) -> bool:
        """True once the arbiter that forked the manager has exited."""
        return os.getppid() != self.parent_pid

    def _tick(self, now: float = None) -> None:
        """One supervision pass over every companion."""
        now = now or time.time()
        self.reap_processes()
        self.retry_backoff(now)
        self.promote_running(now)
        self.enforce_deadlines(now)

    def _wait(self, timeout: float = 1.0) -> None:
        """Block until a signal or a control request arrives, or we time out.

        The self-pipe carries signal wake-ups; the control listener carries
        client connections. Either readable (or the timeout) ends the wait, so
        the next loop pass reacts promptly without busy-spinning.
        """
        readers = [self._wakeup_pipe[0]]
        if self.control is not None and self.control.listener is not None:
            readers.append(self.control.listener)
        try:
            ready, _, _ = select.select(readers, [], [], timeout)
        except (InterruptedError, OSError):
            return
        for reader in ready:
            if reader is self._wakeup_pipe[0]:
                self._drain_wakeup()
            else:
                self._accept_control()

    def _accept_control(self) -> None:
        """Accept one waiting control connection and answer its requests."""
        try:
            connection, _ = self.control.listener.accept()
        except OSError:
            return
        self.control.serve_connection(connection)

    def enforce_deadlines(self, now: float = None) -> None:
        """SIGKILL companions that overran their stop deadline.

        A graceful ``stop_signal`` may be ignored or take too long; once the
        deadline set by stop/restart passes, the companion is force-killed so
        it cannot wedge the manager. The reaper picks up the exit afterwards.
        """
        now = now or time.time()
        for process in self.processes.values():
            if process.state != State.STOPPING or process.stop_deadline is None:
                continue
            if now >= process.stop_deadline and process.pid is not None:
                self._safe_kill(process.pid, signal.SIGKILL)
                process.kill_count += 1
                process.stop_deadline = None
                self.log.warning("companion %s killed after timeout (pid %s)",
                                 process.name, process.pid)

    def stop_all(self) -> None:
        """Stop every companion and reap them as they exit.

        Sends each one its stop signal, then keeps reaping and enforcing stop
        deadlines until they are all gone, so the manager exits without leaving
        orphaned companions behind.
        """
        self.log.info("stopping all companions")
        for name in list(self.processes):
            self.stop_process(name)
        while any(process.pid is not None for process in self.processes.values()):
            now = time.time()
            self.enforce_deadlines(now)
            self.reap_processes()
            self._wait(timeout=0.2)
        self.log.info("all companions stopped")

    def _install_signals(self) -> None:
        """Set up the self-pipe and signal handlers for the supervision loop."""
        self._wakeup_pipe = os.pipe()
        for fd in self._wakeup_pipe:
            util.set_non_blocking(fd)
            util.close_on_exec(fd)
        signal.signal(signal.SIGCHLD, self._wakeup)
        signal.signal(signal.SIGTERM, self._signal_stop)
        signal.signal(signal.SIGINT, self._signal_stop)

    def _signal_stop(self, signum, frame) -> None:
        self.stopping = True
        self._wakeup()

    def _wakeup(self, signum=None, frame=None) -> None:
        """Wake the loop out of ``select`` by writing to the self-pipe."""
        try:
            os.write(self._wakeup_pipe[1], b".")
        except OSError:
            pass

    def _drain_wakeup(self) -> None:
        try:
            while os.read(self._wakeup_pipe[0], 4096):
                pass
        except OSError:
            pass

    def handle_command(self, command: dict) -> dict:
        """Route a decoded control command to its action.

        This is the ``dispatch`` the control socket calls. ``status`` returns a
        snapshot of every companion; ``start``/``stop``/``restart`` act on the
        one named companion and report ``(ok, message)``. Per-companion
        commands need a string ``name``, and anything else raises ``CommandError`` so the
        socket replies with an error envelope.
        """
        command_name = command["cmd"]
        if command_name == "status":
            return {"ok": True, "companions": self.status()}
        if command_name == "reread":
            if self.config_loader is None:
                raise CommandError("reread not configured")
            try:
                new_configs = self.config_loader()
            except Exception as error:
                return {"ok": False, "error": "invalid config: %s" % error,
                        "kept_old_config": True}
            return self.reread_config(new_configs)

        # Every remaining command acts on one named companion.
        name = command.get("name")
        if not isinstance(name, str):
            raise CommandError("'%s' requires a 'name'" % command_name)
        if command_name == "start":
            ok, message = self.start_process(name)
        elif command_name == "stop":
            ok, message = self.stop_process(name)
        elif command_name == "restart":
            ok, message = self.restart_process(name)
        else:
            raise CommandError("unknown command %r" % command_name)
        return {"ok": ok, "message": message}

    def status(self, now: float = None) -> list:
        """Status entry for every companion, for the ``status`` command."""
        now = now or time.time()
        return [process.status_dict(now) for process in self.processes.values()]

    def reread_config(self, new_configs) -> dict:
        """Transactionally apply a fresh set of companion configs.

        Each companion is compared with the running set by ``config_hash``:
        a new name is added and started, a missing name is stopped and removed,
        a changed hash stores the new config and restarts (unless the companion
        was manually stopped, which keeps it STOPPED with the new config ready
        for its next start), and an unchanged hash is left alone. Validation
        runs first, so a bad config touches nothing and the old one stays live.
        """
        try:
            new_by_name = self._index_configs(new_configs)
        except CommandError as e:
            return {"ok": False, "error": str(e), "kept_old_config": True}

        added, removed, restarted, unchanged = [], [], [], []
        old_names = set(self.processes)
        new_names = set(new_by_name)

        for name in old_names - new_names:
            self.stop_process(name)
            del self.processes[name]
            removed.append(name)

        for name in new_names - old_names:
            process = CompanionProcess(new_by_name[name])
            self.processes[name] = process
            self.spawn_process(process)
            added.append(name)

        for name in new_names & old_names:
            process = self.processes[name]
            if process.config.config_hash == new_by_name[name].config_hash:
                unchanged.append(name)
                continue
            process.config = new_by_name[name]
            if process.manual_stop:
                unchanged.append(name)
            else:
                self.restart_process(name)
                restarted.append(name)

        self.log.info(
            "companion reread applied: added %s, removed %s, restarted %s, unchanged %s",
            added, removed, restarted, unchanged)
        return {"ok": True, "added": added, "removed": removed,
                "restarted": restarted, "unchanged": unchanged}

    @staticmethod
    def _index_configs(configs) -> dict:
        """Index configs by name, rejecting duplicates."""
        by_name = {}
        for config in configs:
            if config.name in by_name:
                raise CommandError(
                    "invalid config: duplicate companion name %s" % config.name)
            by_name[config.name] = config
        return by_name

    def spawn_process(self, process: CompanionProcess) -> int:
        """Fork one companion.

        Parent records the pid and moves the companion to STARTING. Child
        resolves and runs the target, exiting the worker on any failure so a
        crashed companion never leaks back into the manager's control flow.

        Spawning is policy-neutral: it does not touch ``manual_stop``. Clearing
        that flag is the job of the commands that intentionally bring a
        companion back (:meth:`start_process`, :meth:`restart_process`), and a
        companion only ever reaches a respawn path with the flag already false.
        """
        pid = os.fork()
        if pid != 0:
            process.pid = pid
            process.state = State.STARTING
            process.started_at = time.time()
            self.log.info("companion %s started (pid %s)", process.name, pid)
            return pid

        try:
            self._close_manager_fds()
            reset_child_signals()
            set_parent_death_signal(signal.SIGTERM)
            if os.getppid() != self.pid:
                # Manager already died between fork and arming: do not run.
                os._exit(0)
            self._apply_environment(process.config)
            self._redirect_output(process.config)
            util._setproctitle("companion [%s]" % process.config.name)
            target = self._resolve_target(process.config.target)
            target()
        except SystemExit:
            raise
        except BaseException:
            self.log.exception("companion %s crashed", process.name)
            os._exit(1)
        os._exit(0)

    def _close_manager_fds(self) -> None:
        """Close the manager's own fds in a freshly forked companion.

        A companion inherits the manager's control socket and wakeup pipe but
        must not keep them: an open listener would let a companion answer
        control requests, and the pipe is the manager's private signal path.
        Both are closed before the target runs.
        """
        if self.control is not None and self.control.listener is not None:
            self.control.listener.close()
        if self._wakeup_pipe is not None:
            for fd in self._wakeup_pipe:
                try:
                    os.close(fd)
                except OSError:
                    pass

    def start_process(self, name: str):
        """Start a companion by name (the control ``start`` command).

        Follows the supervisor-style rules: a STOPPED or BACKOFF companion
        clears its ``manual_stop`` flag, drops any pending retry, and is spawned
        right away. RUNNING and STARTING are already-up, so they report success
        without doing anything. STOPPING is rejected so the caller polls status
        and retries once the old child is gone. Returns ``(ok, message)``.
        """
        process = self.processes.get(name)
        if process is None:
            return False, "unknown companion %s" % name
        if process.state in (State.RUNNING, State.STARTING):
            return True, "%s already %s" % (name, process.state.lower())
        if process.state == State.STOPPING:
            return False, "%s is stopping; retry" % name
        process.manual_stop = False
        process.next_retry_at = None
        self.spawn_process(process)
        return True, "%s started" % name

    def stop_process(self, name: str, now: float = None):
        """Stop a companion by name (the control ``stop`` command).

        Sets ``manual_stop`` so the companion will not auto-restart. A live
        companion (RUNNING or STARTING) is sent its ``stop_signal`` and moved
        to STOPPING with a ``stop_deadline``; the run loop reaps it, or SIGKILLs
        it once the deadline passes. BACKOFF just cancels the pending retry and
        settles in STOPPED. STOPPED and STOPPING are already-there success
        no-ops. Returns ``(ok, message)``.
        """
        process = self.processes.get(name)
        if process is None:
            return False, "unknown companion %s" % name
        process.manual_stop = True
        # A stop must win over an in-flight restart: clearing this keeps
        # handle_exit from respawning a companion the user asked to stop.
        process.restart_pending = False
        if process.state in (State.STOPPED, State.STOPPING):
            return True, "%s already %s" % (name, process.state.lower())
        if process.state == State.BACKOFF:
            process.next_retry_at = None
            process.state = State.STOPPED
            return True, "%s stopped" % name
        now = now or time.time()
        self._safe_kill(process.pid, self._signal_number(process.config.stop_signal))
        process.state = State.STOPPING
        process.stop_deadline = now + process.config.stop_timeout
        self.log.info("companion %s stopping (pid %s)", name, process.pid)
        return True, "%s stopping" % name

    def restart_process(self, name: str, now: float = None):
        """Restart a companion by name (the control ``restart`` command).

        Always clears ``manual_stop`` so the companion comes back. A live
        companion (RUNNING or STARTING) is asked to stop -- it goes STOPPING
        with ``restart_pending`` set and a deadline based on ``reload_timeout``,
        and the reaper respawns it as soon as the old child exits. BACKOFF and
        STOPPED start again immediately. STOPPING is rejected so the caller
        retries. This never rereads config. Returns ``(ok, message)``.
        """
        process = self.processes.get(name)
        if process is None:
            return False, "unknown companion %s" % name
        if process.state == State.STOPPING:
            return False, "%s is stopping; retry" % name
        process.manual_stop = False
        if process.state in (State.RUNNING, State.STARTING):
            now = now or time.time()
            process.restart_pending = True
            self._safe_kill(process.pid, self._signal_number(process.config.stop_signal))
            process.state = State.STOPPING
            process.stop_deadline = now + process.config.reload_timeout
            self.log.info("companion %s restarting (pid %s)", name, process.pid)
            return True, "%s restarting" % name
        process.next_retry_at = None
        self.spawn_process(process)
        return True, "%s started" % name

    @staticmethod
    def _safe_kill(pid: int, sig) -> None:
        """Send ``sig`` to ``pid``, ignoring an already-dead target.

        A companion can exit between the manager deciding to signal it and the
        kill itself (the exit is only reaped on the next tick). Without this the
        resulting ``ProcessLookupError`` would escape and take the manager down.
        """
        try:
            os.kill(pid, sig)
        except ProcessLookupError:
            pass

    @staticmethod
    def _signal_number(stop_signal) -> int:
        """Resolve a stop signal to its number, e.g. ``"SIGTERM"`` -> 15.

        Accepts a signal name or a raw number and validates both against the
        real signal table, so a typo like ``"SIGTRM"`` fails loudly here rather
        than silently sending the wrong signal (or none).
        """
        try:
            if isinstance(stop_signal, str):
                return signal.Signals[stop_signal]
            return signal.Signals(stop_signal)
        except (KeyError, ValueError):
            raise ValueError("unknown stop signal %r" % (stop_signal,))

    def reap_processes(self) -> list:
        """Reap any companions that have exited and record their exit info.

        A companion runs as a forked child of the manager, so when it dies the
        kernel hands its exit status back to us as a zombie until we collect it
        with ``waitpid``. This method does that collecting, and is meant to be
        called once per run-loop tick (typically after a ``SIGCHLD``).

        ``waitpid(-1, WNOHANG)`` asks the kernel for any one dead child without
        blocking. It returns ``(pid, status)`` for a child it reaped, or
        ``(0, 0)`` when children are still alive but none have exited. Several
        companions can die between two ticks, so we loop until one of those two
        stop conditions is hit: ``(0, 0)`` (nothing more to reap right now) or
        ``ChildProcessError`` (no child processes exist at all).

        For each reaped pid we look up its companion, then in order: record the
        exit (signal or code, time, count), free the pid, and move it to its
        next public state via :meth:`handle_exit` -- STOPPED if it was stopped
        on purpose, otherwise BACKOFF for a later restart. Pids we don't
        recognise are ignored. Returns the list of companions reaped this call.
        """
        reaped = []
        while True:
            try:
                pid, status = os.waitpid(-1, os.WNOHANG)
            except ChildProcessError:
                break
            if pid == 0:
                break
            process = self._process_by_pid(pid)
            if process is not None:
                self._record_exit(process, status)
                self._log_exit(process)
                self.handle_exit(process)
                reaped.append(process)
        return reaped

    def _log_exit(self, process: CompanionProcess) -> None:
        """Log how a reaped companion exited, before its fate is decided."""
        if process.last_exit_signal is not None:
            self.log.info("companion %s exited on signal %s",
                          process.name, process.last_exit_signal)
        else:
            self.log.info("companion %s exited with status %s",
                          process.name, process.last_exit_code)

    def handle_exit(self, process: CompanionProcess, now: float = None) -> None:
        """Decide a companion's fate after it exits: restart, stop, or back off.

        A pending restart wins: the old child was asked to stop only so a fresh
        one could take its place, so it is respawned immediately. Otherwise a
        companion that was stopped on purpose settles in STOPPED and stays
        there, and any other exit is unexpected, so it enters BACKOFF and is
        scheduled to restart after a fixed ``restart_delay`` (no exponential
        backoff, no retry cap).
        """
        now = now or time.time()
        if process.restart_pending:
            process.restart_pending = False
            process.restart_count += 1
            self.log.info("companion %s restarting", process.name)
            self.spawn_process(process)
            return
        if process.manual_stop:
            process.state = State.STOPPED
            process.next_retry_at = None
            self.log.info("companion %s stopped", process.name)
            return
        process.state = State.BACKOFF
        process.next_retry_at = now + process.restart_delay
        self.log.info("companion %s backing off, retrying in %ss",
                      process.name, process.restart_delay)

    def retry_backoff(self, now: float = None) -> list:
        """Respawn BACKOFF companions whose fixed retry delay has elapsed.

        Each retry bumps ``restart_count`` and re-forks the companion, which
        puts it back into STARTING. Returns the companions that were retried.
        """
        now = now or time.time()
        retried = []
        for process in self.processes.values():
            if process.state != State.BACKOFF or process.next_retry_at is None:
                continue
            if now >= process.next_retry_at:
                process.restart_count += 1
                process.next_retry_at = None
                self.spawn_process(process)
                retried.append(process)
        return retried

    def promote_running(self, now: float = None) -> list:
        """Move companions that survived ``startsecs`` from STARTING to RUNNING.

        A freshly spawned companion starts in STARTING. If it stays alive for
        its ``startsecs`` window it is considered up and becomes RUNNING; if it
        dies first, reaping handles it instead. Returns the promoted ones.
        """
        now = now or time.time()
        promoted = []
        for process in self.processes.values():
            if process.state != State.STARTING or process.started_at is None:
                continue
            if now - process.started_at >= process.config.startsecs:
                process.state = State.RUNNING
                self.log.info("companion %s running (pid %s)", process.name, process.pid)
                promoted.append(process)
        return promoted

    def _process_by_pid(self, pid: int):
        for process in self.processes.values():
            if process.pid == pid:
                return process
        return None

    @staticmethod
    def _record_exit(process: CompanionProcess, status: int) -> None:
        """Store how a companion died: signal number or exit code, plus time.

        ``status`` is the packed value from ``waitpid``. ``WIFSIGNALED`` tells
        us a signal killed it, in which case ``WTERMSIG`` gives the signal
        number; otherwise it exited normally and ``WEXITSTATUS`` gives its exit
        code. Only one of the two is ever set, so the other is cleared.
        """
        if os.WIFSIGNALED(status):
            process.last_exit_signal = os.WTERMSIG(status)
            process.last_exit_code = None
        else:
            process.last_exit_code = os.WEXITSTATUS(status)
            process.last_exit_signal = None
        process.exited_at = time.time()
        process.exit_count += 1
        process.pid = None

    @staticmethod
    def _apply_environment(config: CompanionConfig) -> None:
        """Apply ``cwd`` and ``env`` in the child before running the target.

        cwd is changed first so a relative path in env (or the target itself)
        resolves against it. env is merged onto the inherited environment, not
        replaced, so the companion keeps the manager's variables.
        """
        if config.cwd:
            os.chdir(config.cwd)
        if config.env:
            os.environ.update(config.env)

    @staticmethod
    def _redirect_output(config: CompanionConfig) -> None:
        """Send the companion's stdout and stderr to its configured log files.

        By default a companion just inherits the manager's stdout/stderr, so
        leaving these unset (or ``"inherit"``) keeps that. Give a file path and
        we append the output there instead. For stderr you can also pass
        ``"stdout"`` to fold the two streams into one file.
        """
        stdout_fd = CompanionManager._open_output(config.stdout)
        if stdout_fd is not None:
            os.dup2(stdout_fd, 1)
            os.close(stdout_fd)
        if config.stderr == "stdout":
            os.dup2(1, 2)
        else:
            stderr_fd = CompanionManager._open_output(config.stderr)
            if stderr_fd is not None:
                os.dup2(stderr_fd, 2)
                os.close(stderr_fd)

    @staticmethod
    def _open_output(value):
        """Open one log file for writing, or return None to leave the stream
        as-is when the companion should keep inheriting it."""
        if value in (None, "inherit"):
            return None
        return os.open(value, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)

    @staticmethod
    def _resolve_target(target: Union[Callable, str]) -> Callable:
        """Return the zero-arg callable for a companion target.

        Accepts an already-callable target or a ``"module:attr"`` import
        string, e.g. ``"frappe_companions:start_rq_default"``.
        """
        if callable(target):
            return target
        module_name, separator, attribute = target.partition(":")
        if not separator:
            raise ValueError("companion target %r must be 'module:callable'" % target)
        return getattr(importlib.import_module(module_name), attribute)
