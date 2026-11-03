Changelog
=========

Unreleased
^^^^^^^^^^

v1.3.0 - November 2, 2026
^^^^^^^^^^^^^^^^^^^^^^^^^^

**New Features:**

* Field values may now be strings and booleans in addition to numbers.  Numbers are serialized exactly as before, so existing files read back unchanged.  Strings are written in double quotes with escaping and booleans as ``true``/``false``.
* ``FieldQuery`` accepts string right-hand sides.  Booleans only compare equal to booleans, and ordering comparisons skip values of a different kind.

**Bug Fixes:**

* Fixed bool-vs-numeric ``FieldQuery`` identity so queries like ``FieldQuery().a == True`` and ``FieldQuery().a == 1`` remain distinct while numeric ``1`` and ``1.0`` remain equivalent.
* Fixed indexed operations for negated ``FieldQuery`` expressions so ``search()``, ``count()``, ``contains()``, ``get()``, ``select()``, ``update()``, and ``remove()`` match storage-scan behavior.

**Performance:**

* Preserved numeric field serialization hot paths while adding support for string and boolean field values.

v1.2.1 - November 2, 2026
^^^^^^^^^^^^^^^^^^^^^^^^^^

**Bug Fixes:**

* Fixed indexed ``FieldQuery.map(...)`` searches so mapped field values are
  evaluated consistently with storage scans.

**CI/CD and Packaging:**

* Updated CI/CD workflows and package metadata to support Python 3.10-3.15.
* Updated README download count to 200,000+.

v1.2.0 - January 26, 2026
^^^^^^^^^^^^^^^^^^^^^^^^^^

**🐍 Python Version Update**

**Breaking Changes:**

* Dropped Python 3.8 support (reached end-of-life October 2024)
* Dropped PyPy support (compatibility issues with modern tooling)
* Minimum required Python version is now 3.9

**Improvements:**

* Fixed unclosed file handles in test suite
* Added ``.venv`` and ``venv`` to ``.gitignore`` and black exclude patterns
* Pinned black to 25.x for consistent formatting across all supported Python versions
* Updated download count to 150,000+

v1.1.1 - September 20, 2025
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**📈 Documentation Enhancement**

**Documentation Improvements:**

* Fixed all Sphinx documentation build errors and formatting issues
* Updated Read the Docs configuration for v2 compliance
* Improved docstring formatting throughout codebase
* Added comprehensive performance section with technical details


v1.1.0 - September 20, 2025
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**🚀 Major Performance and Bug Fix Release**

**New Features:**

* **Optimized insert_multiple Performance**: Added configurable batched processing with ``batch_size`` parameter (default: 1,000)
  
  - Reduces fsync operations from O(n) to O(n/batch_size) for significant performance improvements
  - Memory-efficient iterator-based processing supports large datasets and generators
  - Maintains correct index state management during batch operations
  - Addresses issue #54: "Optimize db.insert_multiple(points) Execution Time"

* **Enhanced Data Validation**: Added comprehensive ``batch_size`` validation with proper error handling

  - ``batch_size`` must be >= 1, raises ``ValueError`` with descriptive message for invalid values
  - Consistent validation across both ``TinyFlux.insert_multiple()`` and ``Measurement.insert_multiple()``

**Bug Fixes:**

* **Fixed Critical Measurement Filtering Bug**: Resolved issue where ``get_field_values()`` returned incorrect results
  
  - Index path was returning field values from all measurements instead of filtering by specified measurement
  - Now ensures perfect parity between storage path and index path behaviors across all get_* methods
  - Fixes issue #58: "get_field_values not working correctly"

**Testing & Quality:**

* Achieved 100% test coverage with 175 passing tests
* Added comprehensive test suites for batching, validation, and measurement filtering
* Verified consistency across all storage types (Memory, CSV) and access patterns
* Enhanced test fixtures to reduce code duplication and improve maintainability

**Documentation:**

* Updated Sphinx documentation with detailed ``batch_size`` parameter usage and performance considerations
* Added examples demonstrating optimal batch sizes for different use cases

v1.0.1 - September 19, 2025
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* Updated Python support: Removed Python 3.7, added Python 3.13 support
* Updated CI/CD workflows to test against Python 3.9-3.13
* Updated GitHub Actions to latest versions for better security and performance
* Updated documentation and package metadata for new Python version requirements
* Fixed decorator bug in temp_storage_op that was introduced in decorator refactoring
* Updated download count to reflect 150,000+ downloads

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