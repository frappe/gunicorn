Fork Information
--------

This is a fork of gunicorn with the following changes:

1. Request timeout implementation for `gthread` - https://github.com/frappe/gunicorn/pull/1 (upstream doesn't have any, we NEED this.)
2. Higher timeout for `selector` - https://github.com/frappe/gunicorn/pull/2 (This is a small optional performance improvement)
3. https://github.com/benoitc/gunicorn/pull/2918 is reverted to avoid connection resets while draining or restarting a worker. 

Note to anyone upgrading/adding changes:
- Pull upstream changes
- Reapply our changes or reconcile our changes
- Update commit/ref in frappe/frappe `pyproject.toml`
- Keep this readme up-to-date with changes. Keep changes small and to the point. 

TODO:

- [ ]  Plan a cleaner solution using custom workers.  All current changes can be used with custom workers but they have the same awkwardness of maintaining a fork since we want to "override" not extend core behavior.  


Gunicorn
--------

.. image:: https://img.shields.io/pypi/v/gunicorn.svg?style=flat
    :alt: PyPI version
    :target: https://pypi.python.org/pypi/gunicorn

.. image:: https://img.shields.io/pypi/pyversions/gunicorn.svg
    :alt: Supported Python versions
    :target: https://pypi.python.org/pypi/gunicorn

.. image:: https://github.com/benoitc/gunicorn/actions/workflows/tox.yml/badge.svg
    :alt: Build Status
    :target: https://github.com/benoitc/gunicorn/actions/workflows/tox.yml

.. image:: https://github.com/benoitc/gunicorn/actions/workflows/lint.yml/badge.svg
    :alt: Lint Status
    :target: https://github.com/benoitc/gunicorn/actions/workflows/lint.yml

Gunicorn 'Green Unicorn' is a Python WSGI HTTP Server for UNIX. It's a pre-fork
worker model ported from Ruby's Unicorn_ project. The Gunicorn server is broadly
compatible with various web frameworks, simply implemented, light on server
resource usage, and fairly speedy.

Feel free to join us in `#gunicorn`_ on `Libera.chat`_.

Documentation
-------------

The documentation is hosted at https://docs.gunicorn.org.

Installation
------------

Gunicorn requires **Python 3.x >= 3.7**.

Install from PyPI::

    $ pip install gunicorn


Usage
-----

Basic usage::

    $ gunicorn [OPTIONS] APP_MODULE

Where ``APP_MODULE`` is of the pattern ``$(MODULE_NAME):$(VARIABLE_NAME)``. The
module name can be a full dotted path. The variable name refers to a WSGI
callable that should be found in the specified module.

Example with test app::

    $ cd examples
    $ gunicorn --workers=2 test:app


Contributing
------------

See `our complete contributor's guide <CONTRIBUTING.md>`_ for more details.


License
-------

Gunicorn is released under the MIT License. See the LICENSE_ file for more
details.

.. _Unicorn: https://bogomips.org/unicorn/
.. _`#gunicorn`: https://web.libera.chat/?channels=#gunicorn
.. _`Libera.chat`: https://libera.chat/
.. _LICENSE: https://github.com/benoitc/gunicorn/blob/master/LICENSE
