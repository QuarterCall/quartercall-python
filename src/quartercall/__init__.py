"""Read QuarterCall Data (data.quartercall.com) into pandas."""

from quartercall._manifest import UnsupportedSchemaError, quarters
from quartercall._version import __version__

__all__ = ["UnsupportedSchemaError", "__version__", "quarters"]
