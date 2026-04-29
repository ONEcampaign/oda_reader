"""Tests for Deflate64 (compression type 9) support.

The OECD occasionally serves bulk download ZIP files compressed with Deflate64,
which Python's stdlib ``zipfile`` does not support out of the box. Our
``_deflate64`` module patches ``zipfile`` to handle this transparently.
"""

import binascii
import io
import struct
import zipfile

import inflate64
import pandas as pd
import pytest

import oda_reader.download._deflate64  # noqa: F401 — ensure patch is active
from oda_reader.download.download_tools import (
    _save_or_return_parquet_files_from_content,
)


def _create_deflate64_zip(files: dict[str, bytes]) -> bytes:  # noqa: PLR0915  # binary ZIP layout is inherently statement-heavy
    """Create a ZIP archive using Deflate64 (type 9) compression.

    Manually constructs the ZIP binary format since Python's ``zipfile``
    cannot *write* Deflate64 — only our patch enables *reading* it.
    """
    buf = io.BytesIO()
    central_dir_entries = []

    for filename, data in files.items():
        d = inflate64.Deflater()
        compressed = d.deflate(data)
        compressed += d.flush()

        local_header_offset = buf.tell()
        crc = binascii.crc32(data) & 0xFFFFFFFF
        fname_bytes = filename.encode("utf-8")

        # Local file header
        buf.write(b"PK\x03\x04")
        buf.write(struct.pack("<H", 20))  # version needed
        buf.write(struct.pack("<H", 0))  # flags
        buf.write(struct.pack("<H", 9))  # compression: Deflate64
        buf.write(struct.pack("<H", 0))  # mod time
        buf.write(struct.pack("<H", 0))  # mod date
        buf.write(struct.pack("<I", crc))
        buf.write(struct.pack("<I", len(compressed)))
        buf.write(struct.pack("<I", len(data)))
        buf.write(struct.pack("<H", len(fname_bytes)))
        buf.write(struct.pack("<H", 0))  # extra field length
        buf.write(fname_bytes)
        buf.write(compressed)

        central_dir_entries.append(
            (fname_bytes, crc, len(compressed), len(data), local_header_offset)
        )

    cd_offset = buf.tell()
    for fname_bytes, crc, comp_size, uncomp_size, offset in central_dir_entries:
        buf.write(b"PK\x01\x02")
        buf.write(struct.pack("<H", 20))  # version made by
        buf.write(struct.pack("<H", 20))  # version needed
        buf.write(struct.pack("<H", 0))  # flags
        buf.write(struct.pack("<H", 9))  # compression: Deflate64
        buf.write(struct.pack("<H", 0))  # mod time
        buf.write(struct.pack("<H", 0))  # mod date
        buf.write(struct.pack("<I", crc))
        buf.write(struct.pack("<I", comp_size))
        buf.write(struct.pack("<I", uncomp_size))
        buf.write(struct.pack("<H", len(fname_bytes)))
        buf.write(struct.pack("<H", 0))  # extra field length
        buf.write(struct.pack("<H", 0))  # comment length
        buf.write(struct.pack("<H", 0))  # disk number start
        buf.write(struct.pack("<H", 0))  # internal attrs
        buf.write(struct.pack("<I", 0))  # external attrs
        buf.write(struct.pack("<I", offset))
        buf.write(fname_bytes)

    cd_size = buf.tell() - cd_offset
    buf.write(b"PK\x05\x06")
    buf.write(struct.pack("<H", 0))  # disk number
    buf.write(struct.pack("<H", 0))  # disk with cd
    buf.write(struct.pack("<H", len(central_dir_entries)))
    buf.write(struct.pack("<H", len(central_dir_entries)))
    buf.write(struct.pack("<I", cd_size))
    buf.write(struct.pack("<I", cd_offset))
    buf.write(struct.pack("<H", 0))  # comment length

    return buf.getvalue()


@pytest.mark.unit
class TestDeflate64Patch:
    """Verify the zipfile monkey-patch works correctly."""

    def test_deflate64_is_recognised(self):
        """Deflate64 (type 9) should not raise after the patch is imported."""
        zipfile._check_compression(9)

    def test_standard_types_still_work(self):
        """STORED, DEFLATED, BZIP2, LZMA must remain functional."""
        for compress_type in (0, 8, 12, 14):
            zipfile._check_compression(compress_type)

    def test_roundtrip_bytes(self):
        """Compress with inflate64, wrap in a ZIP, read back via zipfile."""
        original = b"hello world " * 200
        zip_bytes = _create_deflate64_zip({"data.bin": original})

        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
            assert z.infolist()[0].compress_type == 9
            with z.open("data.bin") as f:
                assert f.read() == original


@pytest.mark.unit
class TestDeflate64FileTypeDetection:
    """Test that _save_or_return_parquet_files_from_content handles Deflate64 ZIPs."""

    def _parquet_bytes(self) -> bytes:
        """Create sample parquet data."""
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        buf = io.BytesIO()
        df.to_parquet(buf)
        return buf.getvalue()

    def test_read_deflate64_parquet(self):
        """Parquet inside a Deflate64 ZIP should be read correctly."""
        zip_bytes = _create_deflate64_zip({"data.parquet": self._parquet_bytes()})

        result = _save_or_return_parquet_files_from_content(zip_bytes)

        assert result is not None
        assert len(result) == 1
        assert isinstance(result[0], pd.DataFrame)
        assert list(result[0].columns) == ["col1", "col2"]
        assert len(result[0]) == 3

    def test_save_deflate64_parquet_to_path(self, tmp_path):
        """Parquet inside a Deflate64 ZIP should save to disk correctly."""
        zip_bytes = _create_deflate64_zip({"data.parquet": self._parquet_bytes()})

        result = _save_or_return_parquet_files_from_content(
            zip_bytes, save_to_path=tmp_path
        )

        assert result is None
        saved = list(tmp_path.glob("*.parquet"))
        assert len(saved) == 1
        df = pd.read_parquet(saved[0])
        assert len(df) == 3

    def test_read_deflate64_csv(self):
        """CSV inside a Deflate64 ZIP should be read correctly."""
        csv_data = b"col1,col2,col3\n1,2,3\n4,5,6\n"
        zip_bytes = _create_deflate64_zip({"data.csv": csv_data})

        result = _save_or_return_parquet_files_from_content(zip_bytes)

        assert result is not None
        assert len(result) == 1
        df = result[0]
        assert list(df.columns) == ["col1", "col2", "col3"]
        assert len(df) == 2

    def test_save_deflate64_csv_as_parquet(self, tmp_path):
        """CSV inside a Deflate64 ZIP should convert to parquet when saving."""
        csv_data = b"col1|col2|col3\n1|2|3\n4|5|6\n"
        zip_bytes = _create_deflate64_zip({"data.csv": csv_data})

        result = _save_or_return_parquet_files_from_content(
            zip_bytes, save_to_path=tmp_path
        )

        assert result is None
        saved = list(tmp_path.glob("*.parquet"))
        assert len(saved) == 1
        df = pd.read_parquet(saved[0])
        assert len(df) == 2
        assert list(df.columns) == ["col1", "col2", "col3"]

    def test_read_deflate64_txt(self):
        """TXT inside a Deflate64 ZIP should be read correctly."""
        txt_data = b"col1,col2\na,1\nb,2\n"
        zip_bytes = _create_deflate64_zip({"data.txt": txt_data})

        result = _save_or_return_parquet_files_from_content(zip_bytes)

        assert result is not None
        assert len(result) == 1
        assert len(result[0]) == 2
