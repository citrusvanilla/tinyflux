"""
This module contains decorators for various operations related to \
storage management.

The decorators ensure certain preconditions are met before executing methods,
such as checking if the storage is appendable, readable, or writable. They also
handle temporary storage initialization and cleanup when required.

Decorators included:
- `append_op`: Ensures storage can be appended to before executing the method.
- `read_op`: Ensures storage can be read from, with optional reindexing.
- `temp_storage_op`: Handles temporary storage initialization and cleanup for
  operations.
- `write_op`: Ensures storage can be written to before executing the method.
"""

from functools import wraps
from typing import (
    Any,
    Callable,
)


def append_op(method: Callable[..., Any]) -> Callable[..., Any]:
    """Decorate an append operation with assertion.

    Ensures storage can be appended to before doing anything.
    """

    @wraps(method)
    def op(self: Any, *args: Any, **kwargs: Any) -> Any:
        """Decorate."""
        assert self._storage.can_append
        return method(self, *args, **kwargs)

    return op


def read_op(method: Callable[..., Any]) -> Callable[..., Any]:
    """Decorate a read operation with assertion.

    Ensures storage can be read from before doing anything.
    """

    @wraps(method)
    def op(self: Any, *args: Any, **kwargs: Any) -> Any:
        """Decorate."""
        assert self._storage.can_read

        if self._auto_index and not self._index.valid:
            self.reindex()

        return method(self, *args, **kwargs)

    return op


def temp_storage_op(method: Callable[..., Any]) -> Callable[..., Any]:
    """Decorate a db operation that requires auxiliary storage.

    Initializes temporary storage, invokes method, and cleans-up storage after
    op has run.
    """

    @wraps(method)
    def op(self: Any, *args: Any, **kwargs: Any) -> Any:
        """Decorate."""
        # Init temp storage in the storage class.
        self._storage._init_temp_storage()

        # Invoke op.
        rst = method(self, *args, **kwargs)

        # Clean-up temp storage.
        self._storage._cleanup_temp_storage()

        return rst

    return op


def write_op(method: Callable[..., Any]) -> Callable[..., Any]:
    """Decorate a write operation with assertion.

    Ensures storage can be written to before doing anything.
    """

    @wraps(method)
    def op(self: Any, *args: Any, **kwargs: Any) -> Any:
        """Decorate."""
        assert self._storage.can_write
        return method(self, *args, **kwargs)

    return op
