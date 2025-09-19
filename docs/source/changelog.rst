Changelog
=========

v1.0.1 - September 19, 2025
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* Updated Python support: Removed Python 3.7, added Python 3.13 support
* Updated CI/CD workflows to test against Python 3.8-3.13
* Updated GitHub Actions to latest versions for better security and performance
* Updated documentation and package metadata for new Python version requirements
* Fixed decorator bug in temp_storage_op that was introduced in decorator refactoring
* Updated download count to reflect 120,000+ downloads

v1.0.0 - April 13, 2024
^^^^^^^^^^^^^^^^^^^^^^^

* Official Release 🎉. TinyFlux has been stable for over 20 months.


v0.4.1 - September 25, 2023
^^^^^^^^^^^^^^^^^^^^^^^^^^^

* Spelling bug fix in support of issue #44.


v0.4.0 - March 27, 2023
^^^^^^^^^^^^^^^^^^^^^^^

* Tags and Fields can be removed from individual points. See `the documentation <https://tinyflux.readthedocs.io/en/latest/updating-data.html#removing-tags-and-fields-with-update>`__ for more (resolves issue #27).


v0.3.1 (2023-3-27)
^^^^^^^^^^^^^^^^^^

* Fixed bug that allowed user to delete key/field tags with `.update()` and `.update_all()`. (resolves issue #36).


v0.3.0 (2023-3-21)
^^^^^^^^^^^^^^^^^^

* Tag and field keys can be compacted when using CSVStorage, saving potentially many bytes per Point (resolves issue #32).
* Fixed bug that causes tag values of '' to be serialized as "_none" (resolves issue #33).


v0.2.6 (2023-3-9)
^^^^^^^^^^^^^^^^^

* TinyFlux is now PEP 561 compliant (resolves issue #31).

v0.2.4 (2023-2-15)
^^^^^^^^^^^^^^^^^^

* Fix bug that prevents updating Points when using a CSVStorage instance.


v0.2.1 (2022-11-22)
^^^^^^^^^^^^^^^^^^^

* Fix bug that caused values of 0.0 to be serialized as None/null rather than "0.0".


v0.2.0 (2022-11-09)
^^^^^^^^^^^^^^^^^^^

* Test and verification on Python 3.11 and Windows platforms
* Disable universal newlines translation on CSV Storage instances


v0.1.0 (2022-05-16)
^^^^^^^^^^^^^^^^^^^

* Initial release