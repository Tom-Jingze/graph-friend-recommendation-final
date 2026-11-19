"""Fetch the official SNAP static YouTube graph with provenance and SHA-256.

The digest records the downloaded compressed bytes; SNAP does not publish an
authenticated SHA-256 on the dataset card. --expected-sha256 can pin a prior
known download. This script does not create synthetic timestamps or scale data.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import gzip
import hashlib
import json
import os
from pathlib import Path
import tempfile
from urllib.request import Request, urlopen

URL = "https://snap.stanford.edu/data/bigdata/communities/com-youtube.ungraph.txt.gz"
SOURCE = "https://snap.stanford.edu/data/com-Youtube.html"


def fetch(output: Path, expected_sha256: str | None = None) -> dict:
    output = output.resolve()
    metadata_path = output.with_suffix(output.suffix + ".source.json")
    if output.exists() or metadata_path.exists():
        raise FileExistsError(f"Refusing to overwrite data or provenance: {output}")
    if expected_sha256 is not None:
        expected_sha256 = expected_sha256.lower()
        if len(expected_sha256) != 64 or any(char not in "0123456789abcdef" for char in expected_sha256):
            raise ValueError("expected-sha256 must be 64 hexadecimal characters")
    output.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=".youtube-download-", suffix=".part", dir=output.parent)
    temporary = Path(temporary_name)
    digest, size = hashlib.sha256(), 0
    try:
        with os.fdopen(descriptor, "wb") as destination:
            with urlopen(Request(URL, headers={"User-Agent": "graph-friend-recommendation-v2/1.0"}), timeout=60) as response:
                final_url = response.geturl()
                content_length = response.headers.get("Content-Length")
                last_modified = response.headers.get("Last-Modified")
                for block in iter(lambda: response.read(1024 * 1024), b""):
                    destination.write(block)
                    digest.update(block)
                    size += len(block)
            destination.flush()
            os.fsync(destination.fileno())
        if content_length is not None and size != int(content_length):
            raise ValueError(f"Incomplete download: expected {content_length} bytes, got {size}")
        actual_sha256 = digest.hexdigest()
        if expected_sha256 is not None and actual_sha256 != expected_sha256:
            raise ValueError(f"SHA-256 mismatch: received {actual_sha256}")
        # Reading to EOF verifies the gzip trailer/CRC before publication.
        with gzip.open(temporary, "rb") as compressed:
            for _ in iter(lambda: compressed.read(1024 * 1024), b""):
                pass
        metadata = {
            "dataset": "SNAP com-Youtube undirected social graph",
            "source_page": SOURCE, "download_url": URL, "resolved_download_url": final_url,
            "downloaded_at_utc": datetime.now(timezone.utc).isoformat(),
            "http_last_modified": last_modified, "compressed_bytes": size,
            "sha256_compressed_file": actual_sha256, "gzip_crc_verified": True,
            "expected_sha256_supplied": expected_sha256 is not None,
            "dataset_card_statistics": {"nodes": 1_134_890, "undirected_edges": 2_987_624},
            "statistics_independently_counted": False,
            "citation": "J. Yang and J. Leskovec. Defining and Evaluating Network Communities based on Ground-truth. ICDM 2012.",
            "limitations": ["Static graph without real edge event timestamps.",
                           "SHA-256 records downloaded bytes; it is not an official signed digest.",
                           "Full-graph download does not demonstrate full-graph Spark execution; see run manifests."],
        }
        # Windows rename refuses an existing destination and works without the
        # hard-link privilege required in some Windows sandboxes. POSIX rename
        # would replace an existing file, so use exclusive hard-link creation.
        if os.name == "nt":
            os.rename(temporary, output)
        else:
            os.link(temporary, output)
        with metadata_path.open("x", encoding="utf-8") as handle:
            json.dump(metadata, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
        return metadata
    finally:
        temporary.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("data/external/com-youtube.ungraph.txt.gz"))
    parser.add_argument("--expected-sha256")
    args = parser.parse_args()
    print(json.dumps(fetch(args.output, args.expected_sha256), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
