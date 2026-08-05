"""Unit tests for `validate_payload_or_raise`'s magic-byte dispatch.

Covers the zip path, the bare-parquet path (for CRS.parquet /
CRS-reduced.parquet), and the unrecognised-payload fallback.
"""

import zipfile

import pandas as pd
import pytest

from oda_reader.exceptions import (
    BulkPayloadCorruptError,
    validate_payload_or_raise,
    validate_zip_or_raise,
)


@pytest.mark.unit
class TestValidatePayloadOrRaise:
    def test_valid_zip_passes(self, tmp_path):
        target = tmp_path / "good.zip"
        with zipfile.ZipFile(target, "w") as zf:
            zf.writestr("data.txt", "hello")

        validate_payload_or_raise(target)  # must not raise

        assert target.exists()

    def test_valid_parquet_passes(self, tmp_path):
        target = tmp_path / "good.parquet"
        pd.DataFrame({"a": [1, 2, 3]}).to_parquet(target)

        validate_payload_or_raise(target)  # must not raise

        assert target.exists()

    def test_garbage_payload_raises_and_unlinks(self, tmp_path):
        target = tmp_path / "garbage.bin"
        target.write_bytes(b"this is not a zip or a parquet file at all")

        with pytest.raises(BulkPayloadCorruptError) as exc_info:
            validate_payload_or_raise(target)

        assert "unrecognised magic bytes" in exc_info.value.reason
        assert not target.exists()

    def test_corrupt_zip_raises_and_unlinks(self, tmp_path):
        target = tmp_path / "bad.zip"
        # Zip magic bytes, but not a real zip structure.
        target.write_bytes(b"PK\x03\x04" + b"\x00" * 20)

        with pytest.raises(BulkPayloadCorruptError):
            validate_payload_or_raise(target)

        assert not target.exists()

    def test_parquet_with_garbage_footer_raises_and_unlinks(self, tmp_path):
        target = tmp_path / "bad.parquet"
        # Leading and trailing PAR1 magic (so header/footer checks pass), but
        # the footer bytes in between aren't a real Thrift-encoded metadata
        # block -- the pq.ParquetFile() metadata read must fail.
        target.write_bytes(b"PAR1" + b"\x00" * 50 + b"PAR1")

        with pytest.raises(BulkPayloadCorruptError):
            validate_payload_or_raise(target)

        assert not target.exists()

    def test_validate_zip_or_raise_is_the_same_function(self):
        """The old name is a true alias, not a zip-only remnant."""
        assert validate_zip_or_raise is validate_payload_or_raise
