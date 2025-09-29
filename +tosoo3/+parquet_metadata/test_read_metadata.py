#!/usr/bin/env python3
"""
Read MATLAB-written Parquet file and validate user key_value metadata using PyArrow.

Usage:
  python3 test_read_metadata.py [path/to/file.parquet]

If no path is given, defaults to ./matlab_roundtrip.parquet
"""

import sys
import json
from pathlib import Path

try:
    import pyarrow.parquet as pq
except Exception as e:
    print("ERROR: pyarrow is required to run this test. Install with: pip install pyarrow")
    raise


def to_text(b: bytes) -> str:
    if isinstance(b, bytes):
        try:
            return b.decode("utf-8")
        except Exception:
            return b.decode("latin1", errors="replace")
    return str(b)


def maybe_json(s: str):
    t = s.strip()
    if t.startswith("{") or t.startswith("[") or t in ("true", "false") or t.isdigit():
        try:
            return json.loads(s)
        except Exception:
            return s
    return s


def main():
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("test_files/matlab_roundtrip.parquet")
    if not path.exists():
        print(f"ERROR: File not found: {path}")
        sys.exit(2)

    pf = pq.ParquetFile(str(path))
    md = pf.metadata.metadata or {}

    # Convert bytes->str and maybe decode JSON where appropriate
    out = {}
    for k, v in md.items():
        ks = to_text(k)
        vs = to_text(v)
        out[ks] = maybe_json(vs)

    # Determine expected values based on file name
    if "inplace" in path.name:
        exp_author = "matlab-writer-inplace"
        exp_ver = 2
        exp_alpha = 0.2
        exp_beta = False
        exp_tags = ["x", "y"]
    elif "roundtrip_brotli" in path.name:
        exp_author = "matlab-writer-brotli"
        exp_ver = 3
        exp_alpha = 0.3
        exp_beta = True
        exp_tags = ["brotli", "ok"]
    else:
        exp_author = "matlab-writer"
        exp_ver = 1
        exp_alpha = 0.1
        exp_beta = True
        exp_tags = ["a", "b"]

    # Basic validations to match MATLAB tests
    assert out.get("project") == "parquetmatlab", f"project mismatch: {out.get('project')}"
    assert out.get("author") == exp_author, f"author mismatch: {out.get('author')} vs {exp_author}"

    ver = out.get("version_num")
    assert ver in (exp_ver, str(exp_ver)), f"version_num mismatch: {ver} vs {exp_ver}"

    cfg = out.get("config")
    if isinstance(cfg, str):
        try:
            cfg = json.loads(cfg)
        except Exception:
            pass
    assert isinstance(cfg, dict), "config is not a dict"
    assert abs(float(cfg.get("alpha", -1)) - float(exp_alpha)) < 1e-9, f"alpha mismatch: {cfg}"
    assert bool(cfg.get("beta")) == bool(exp_beta), f"beta mismatch: {cfg}"
    tags = cfg.get("tags")
    assert isinstance(tags, (list, tuple)) and tags[:2] == exp_tags, f"tags mismatch: {tags} vs {exp_tags}"

    print("Python interop metadata test passed.")


if __name__ == "__main__":
    main()
