Tooling and Conventions
=======================

TinyFlux should be developed locally with the latest stable version of Python on any platform  (3.10 as of this writing).


Versioning
----------

TinyFlux follows `semantic versioning`_ guidelines for releases.


Workflow
--------

TinyFlux development follows the branch-based workflow known as "`GitHub flow`_".


Continuous Integration and Deployment
-------------------------------------

TinyFlux uses `GitHub Actions`_ for its CI/CD workflow.


Coding Conventions
------------------

TinyFlux conforms to `PEP 8`_ for style, and `Google Python Style Guide`_ for docstrings.  TinyFlux uses common developer tools to check and enforce this.  These checks should be performed locally before pushing to GitHub, as they will eventually be enforced with GitHub Actions (see ``.github/workflows`` in the TinyFlux GitHub repository for details).


Formatting
^^^^^^^^^^

TinyFlux uses standard configuration black_ for code formatting, with an enforced line-length of 79 characters.


Style
^^^^^

TinyFlux uses standard configuration flake8_ for style enforcement, with an enforced line-length of 79 characters.


Typing
^^^^^^

TinyFlux uses standard configuration mypy_ for static type checking.


Documentation
^^^^^^^^^^^^^

TinyFlux hosts documentation on `Read The Docs`_.

TinyFlux uses Sphinx_ for documentation generation, with a customized `Read the Docs Sphinx Theme`_, enabled for "Google-style" docstrings.

Testing
-------

TinyFlux aims for 100% code coverage through unit testing.


Test Framework
^^^^^^^^^^^^^^

TinyFlux uses pytest_ as its testing framework.


Coverage
^^^^^^^^

TinyFlux uses Coverage.py_ for measuring code coverage.




.. _PEP 8: https://peps.python.org/pep-0008/
.. _Google Python Style Guide: https://google.github.io/styleguide/pyguide.html
.. _black: https://black.readthedocs.io/en/stable/
.. _flake8: https://flake8.pycqa.org/en/latest/
.. _mypy: https://mypy.readthedocs.io/en/stable/
.. _Sphinx: https://www.sphinx-doc.org/en/master/
.. _Read the Docs Sphinx Theme: https://sphinx-rtd-theme.readthedocs.io/en/stable/
.. _pytest: https://docs.pytest.org/en/7.1.x/
.. _Coverage.py: https://coverage.readthedocs.io/en/6.3.3/
.. _GitHub Actions: https://docs.github.com/en/actions
.. _Read the Docs: https://readthedocs.org/
.. _semantic versioning: https://semver.org/
.. _GitHub flow: https://docs.github.com/en/get-started/quickstart/github-flow