"""Patch Python's zipfile module to support Deflate64 (type 9) decompression.

The OECD occasionally serves bulk download ZIP files compressed with Deflate64,
which Python's stdlib ``zipfile`` does not support. This module patches
``zipfile._get_decompressor`` and ``zipfile._check_compression`` to handle
Deflate64 using the ``inflate64`` library.

Only *decompression* is patched — we never need to *write* Deflate64 archives.

Import this module for the side-effect::

    import oda_reader.download._deflate64  # noqa: F401
"""

import zipfile

import inflate64

_DEFLATE64 = 9
_original_check = zipfile._check_compression
_original_decompressor = zipfile._get_decompressor


class _Deflate64Decompressor:
    """Adapter that wraps ``inflate64.Inflater`` to match the interface
    expected by ``zipfile.ZipExtFile``: a ``decompress(data)`` method
    and an ``eof`` attribute."""

    def __init__(self):
        self._inflater = inflate64.Inflater()

    def decompress(self, data):
        return self._inflater.inflate(data)

    @property
    def eof(self):
        return self._inflater.eof


def _check_compression_with_deflate64(compression):
    if compression == _DEFLATE64:
        return
    return _original_check(compression)


def _get_decompressor_with_deflate64(compress_type):
    if compress_type == _DEFLATE64:
        return _Deflate64Decompressor()
    return _original_decompressor(compress_type)


zipfile._check_compression = _check_compression_with_deflate64
zipfile._get_decompressor = _get_decompressor_with_deflate64
