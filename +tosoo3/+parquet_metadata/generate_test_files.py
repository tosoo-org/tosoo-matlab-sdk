#!/usr/bin/env python3
"""
Generate Parquet files with user key_value metadata that match the
expectations in test_all_files.m.

Files created in repo root:
  - test_1_simple_metadata.parquet
  - test_2_json_metadata.parquet
  - test_3_mixed_types.parquet
  - test_4_special_characters.parquet
  - test_5_large_metadata.parquet
  - test_6_edge_cases.parquet
"""

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


ROOT = Path(__file__).resolve().parent
OUT = ROOT / "test_files"
OUT.mkdir(exist_ok=True)


def w(path: Path, metadata: dict):
    """Write a tiny table with given file-level metadata (user KV).

    In PyArrow, set file key_value_metadata by attaching bytes metadata to the
    table schema (schema.metadata) before writing.
    """
    # Minimal content table
    tbl = pa.table({"id": pa.array([1, 2, 3], type=pa.int32())})
    # Ensure metadata keys/values are bytes
    meta_bytes = {
        (k if isinstance(k, (bytes, bytearray)) else str(k).encode("utf-8")):
        (v if isinstance(v, (bytes, bytearray)) else str(v).encode("utf-8"))
        for k, v in metadata.items()
    }
    tbl = tbl.replace_schema_metadata(meta_bytes)
    pq.write_table(tbl, str(path))


def gen_test1():
    path = OUT / "test_1_simple_metadata.parquet"
    md = {
        "title": "Simple Test Dataset",
        "description": "Basic metadata test with simple strings",
        "author": "Test Generator",
        "version": "1.0.0",
        "created_date": "2024-01-01",
        "data_source": "synthetic",
        "processing_notes": "Generated for MATLAB metadata reader testing",
    }
    w(path, md)


def gen_brotli():
    path = OUT / "brotli_base.parquet"
    tbl = pa.table({
        "id": pa.array(list(range(1, 1001)), type=pa.int32()),
        "text": pa.array([f"row_{i}" for i in range(1, 1001)], type=pa.string()),
    })
    meta = {
        b"title": b"Brotli Base Test",
        b"author": b"pyarrow",
        b"project": b"parquetmatlab",
        b"compression": b"brotli",
        b"version": b"1",
    }
    tbl = tbl.replace_schema_metadata(meta)
    pq.write_table(tbl, str(path), compression="brotli")


def gen_test2():
    path = OUT / "test_2_json_metadata.parquet"
    configuration = {
        "algorithm": {
            "name": "advanced_processor",
            "version": "2.1.3",
            "parameters": {
                "threshold": 0.85,
                "iterations": 1000,
                "learning_rate": 0.001,
                "regularization": {"l1": 0.01, "l2": 0.005},
            },
        },
        "preprocessing": {
            "normalization": True,
            "outlier_removal": True,
            "feature_scaling": "minmax",
        },
    }
    experiment_setup = {
        "id": "EXP_2024_001",
        "conditions": ["control", "treatment_A", "treatment_B"],
        "sample_sizes": [50, 75, 100],
    }
    md = {
        "title": "JSON Metadata Test",
        "author": "Complex Test Generator",
        "configuration": json.dumps(configuration),
        "experiment_setup": json.dumps(experiment_setup),
    }
    w(path, md)


def gen_test3():
    path = OUT / "test_3_mixed_types.parquet"
    mixed_config = {
        "string_field": "test_value",
        "integer_field": 42,
        "float_field": 3.14159,
        "boolean_field": True,
        "array_strings": ["one", "two", "three"],
        "array_numbers": [1, 2, 3, 4, 5],
        "nested_object": {"level_2": {"level_3": {"deep_value": "found_me"}}},
    }
    md = {
        "title": "Mixed Types Test",
        "simple_string": "This is just a string",
        "simple_number_as_string": "12345",
        "mixed_config": json.dumps(mixed_config),
    }
    w(path, md)


def gen_test4():
    path = OUT / "test_4_special_characters.parquet"
    special_content = {
        "unicode_text": "Café naïve façade — 東京",
        "emoji": "😊🚀✨",
    }
    md = {
        "title": "Special Characters Test",
        "key with spaces": "Spaces in key name",
        "key-with-dashes": "Dashes in key",
        "key_with_underscores": "Underscores in key",
        "key.with.dots": "Dots in key",
        "UPPERCASE_KEY": "Uppercase key",
        "MiXeD_cAsE_KeY": "Mixed case key",
        "special_content": json.dumps(special_content, ensure_ascii=False),
    }
    w(path, md)


def gen_test5():
    path = OUT / "test_5_large_metadata.parquet"
    large_array = [{"i": i, "value": f"item_{i}"} for i in range(2000)]
    large_text = "X" * 65000  # > 60K chars
    data_dict = {
        "columns": {
            "col1": {"type": "int", "desc": "id"},
            "col2": {"type": "string", "desc": "name"},
            "col3": {"type": "float", "desc": "score"},
            "col4": {"type": "bool", "desc": "flag"},
            "col5": {"type": "string", "desc": "notes"},
        }
    }
    md = {
        "title": "Large Metadata Test",
        "large_json_array": json.dumps(large_array),
        "large_text_content": large_text,
        "data_dictionary": json.dumps(data_dict),
    }
    w(path, md)


def gen_test6():
    path = OUT / "test_6_edge_cases.parquet"
    edge_cases = {
        "scientific_notation": {"small": 1.23e-10, "large": 6.02e23}
    }
    md = {
        "title": "Edge Cases Test",
        "empty_string": "",
        "whitespace_only": "   ",
        "just_braces": "{}",  # should parse to empty struct
        "just_brackets": "[]",  # relaxed handling
        "invalid_json_like": "{this is not json}",
        "edge_cases": json.dumps(edge_cases),
    }
    w(path, md)


def main():
    gen_test1()
    gen_test2()
    gen_test3()
    gen_test4()
    gen_test5()
    gen_test6()
    # Brotli-compressed column data with basic metadata
    gen_brotli()
    print("Generated test parquet files in:", ROOT)


if __name__ == "__main__":
    main()
