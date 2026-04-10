"""Tests for the archive parser (ZIP/TAR VFS sandbox)."""

from __future__ import annotations

import io
import tarfile
import zipfile

import pytest

from src.core.models import FileIdentity, FileRecord
from src.parsers.archive_ext import ArchiveParser


@pytest.fixture
def parser():
    return ArchiveParser()


def _make_record(path: str) -> FileRecord:
    return FileRecord(
        identity=FileIdentity(path=path),
        mime_type="application/zip",
    )


def _create_zip(files: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return buf.getvalue()


def _create_tar(files: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tf:
        for name, content in files.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(content)
            tf.addfile(info, io.BytesIO(content))
    return buf.getvalue()


@pytest.mark.asyncio
async def test_can_parse_zip(parser):
    assert await parser.can_parse(_make_record("/test.zip"))


@pytest.mark.asyncio
async def test_can_parse_tar(parser):
    rec = FileRecord(
        identity=FileIdentity(path="/test.tar.gz"),
        mime_type="application/gzip",
    )
    assert await parser.can_parse(rec)


@pytest.mark.asyncio
async def test_parse_zip_basic(parser):
    data = _create_zip({"a.txt": b"hello", "b.txt": b"world"})
    record = _make_record("/test.zip")
    result = await parser.parse(record, data)

    assert result.metadata["type"] == "zip"
    assert result.metadata["file_count"] == 2
    assert len(result.sub_files) == 2
    assert b"hello" in result.sub_files[0] or b"hello" in result.sub_files[1]


@pytest.mark.asyncio
async def test_parse_tar_basic(parser):
    data = _create_tar({"a.txt": b"hello", "b.txt": b"world"})
    record = FileRecord(
        identity=FileIdentity(path="/test.tar"),
        mime_type="application/x-tar",
    )
    result = await parser.parse(record, data)

    assert result.metadata["type"] == "tar"
    assert result.metadata["file_count"] == 2


@pytest.mark.asyncio
async def test_zip_depth_limit(parser):
    parser.max_depth = 1
    files = {"deep/nested/file.txt": b"too deep"}
    data = _create_zip(files)
    record = _make_record("/test.zip")
    result = await parser.parse(record, data)

    # depth=3 exceeds max_depth=1
    assert result.metadata["file_count"] == 0


@pytest.mark.asyncio
async def test_zip_file_count_limit(parser):
    parser.max_files = 2
    files = {f"file_{i}.txt": b"data" for i in range(10)}
    data = _create_zip(files)
    record = _make_record("/test.zip")
    result = await parser.parse(record, data)

    assert result.metadata["file_count"] == 2


@pytest.mark.asyncio
async def test_zip_size_limit(parser):
    parser.max_unpack_size = 10  # 10 bytes
    files = {"big.txt": b"A" * 100}
    data = _create_zip(files)
    record = _make_record("/test.zip")
    result = await parser.parse(record, data)

    assert result.metadata["file_count"] == 0


def test_lane_is_background(parser):
    assert parser.lane == "background"
