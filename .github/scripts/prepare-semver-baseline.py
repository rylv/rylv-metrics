"""Prepare the published baseline with the rustix 1.1.5 feature workaround."""

import io
import json
from pathlib import Path
import re
import sys
import tarfile
import urllib.request


def download(url):
    request = urllib.request.Request(url, headers={"User-Agent": "rylv-metrics-ci"})
    with urllib.request.urlopen(request, timeout=60) as response:
        return response.read()


metadata = json.loads(download("https://crates.io/api/v1/crates/rylv-metrics"))
version = metadata["crate"]["max_stable_version"]
if not re.fullmatch(r"\d+\.\d+\.\d+", version):
    raise ValueError(f"Unexpected baseline version: {version!r}")

archive = download(f"https://crates.io/api/v1/crates/rylv-metrics/{version}/download")
destination = Path(sys.argv[1]).resolve()
destination.mkdir(parents=True, exist_ok=True)
with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as package:
    package.extractall(destination, filter="data")

baseline = destination / f"rylv-metrics-{version}"
if version == "0.3.2":
    # This published manifest predates rustix 1.1.5. Only enable its missing
    # dependency feature; keep all baseline source files and public APIs intact.
    manifest = baseline / "Cargo.toml"
    original = manifest.read_text()
    patched, replacements = re.subn(
        r'(\[dependencies\.rustix\]\nversion = "1\.1\.2"\nfeatures = \[)',
        r'\1\n    "time",',
        original,
    )
    if replacements != 1:
        raise ValueError("Published rustix dependency did not match the expected manifest")
    manifest.write_text(patched)

print(f"path={baseline}")
