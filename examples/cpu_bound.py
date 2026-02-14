from gunicorn import __version__
import time

def burn():
    # unsure whether to read from an actual database read which would require preparation
    # instead, we're sleeping to simulate the wait on an external resource
    time.sleep(5)

    # basic cpu-bound operation
    sum = 0
    for i in range(10000000):
        sum += i ** 2
    return sum

def app(environ, start_response):
    status = '200 OK'
    response_headers = [
        ('Content-type', 'text/plain'),
        ('X-Gunicorn-Version', __version__)
    ]
    start_response(status, response_headers)

    x = burn()
    body = (str(x) + "\n").encode('utf-8')
    return [body]