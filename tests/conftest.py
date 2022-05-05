"""PyTest configuration and test fixtures."""
import pytest

from tinyflux.storages import CSVStorage, MemoryStorage


class CSVStorageWithCounters(CSVStorage):
    """CSVStorage with some counters for read/write/append ops."""

    def __init__(self, *args, **kwargs):
        """Init this class."""
        super().__init__(*args, **kwargs)
        self.reindex_count = 0
        self.write_count = 0
        self.append_count = 0

    def append(self, points):
        """Append with counter."""
        self.append_count += 1
        return super().append(points)

    def _write(self, points, is_sorted=False):
        """Write with counter."""
        self.write_count += 1
        return super()._write(points, is_sorted)


class MemoryStorageWithCounters(MemoryStorage):
    """MemoryStorage with some counters for read/write/append ops."""

    def __init__(self):
        """Init a MemoryStorage instance."""
        super().__init__()
        self.append_count = 0
        self.reindex_count = 0
        self.write_count = 0

    def append(self, points):
        """Append with counter."""
        self.append_count += 1
        return super().append(points)

    def _write(self, points, is_sorted=False):
        """Write with counter."""
        self.write_count += 1
        return super()._write(points, is_sorted)


@pytest.fixture
def mem_storage_with_counters():
    """Return a MemoryStorage class with counters for read/write/append."""
    return MemoryStorageWithCounters


@pytest.fixture
def csv_storage_with_counters():
    """Return a CSVStorage class with counters for read/write/append."""
    return CSVStorageWithCounters
