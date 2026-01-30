# Basic tests for parse_nas_log_levels.py
# Run with: pytest -q
from pathlib import Path
from parse_nas_log_levels import parse_line, build_level_component_counts, to_rows, LogRecordLite

def test_parse_line_nominal():
    line = '2024-10-25 01:44:23.024 +00:00 [Verbose    ] [NAS                 ] [T=12,ET=] Creating client proxy'
    rec = parse_line(line)
    assert rec and rec.level.strip() == "Verbose" and rec.component == "NAS"

def test_parse_line_fallback():
    # Missing timestamp; fallback grabs first two bracketed tokens
    line = '[Information] [SCF] something without a timestamp'
    rec = parse_line(line)
    assert isinstance(rec, LogRecordLite)
    assert rec.level == "Information"
    assert rec.component == "SCF"


def test_nas_log_level_component_counts():
    from parse_nas_log_levels import build_level_component_counts
    import os
    base = os.path.join(os.path.dirname(__file__), "Nas_Tests")
    files = [
        ("ex1.txt", {
            "Information": {"NAS": 2, "SCF": 1, "ConfigurationFileWatcher": 1},
            "Verbose": {"NAS": 4, "NasManager": 3, "ConfigurationFileWatcher": 11, "SCSIGateway": 3, "TRACE": 1},
            "Debug": {"NAS": 2, "NasManager": 8, "ConfigurationFileWatcher": 1},
            "Error": {"NasManager": 1},
        }),
        ("ex2.txt", {
            "Verbose": {"NasManager": 15, "ObjectStore": 7},
            "Debug": {"StopwatchScope": 6, "NasManager": 4},
            "Information": {"ObjectStore": 3},
        }),
        ("ex3.txt", {
            "Warning": {"NasManager": 12},
            "Debug": {"StopwatchScope": 16},
            "Verbose": {"NasManager": 21},
            "Information": {"NAS": 2},
        }),
    ]
    for fname, expected in files:
        path = Path(base) / fname
        mapping = build_level_component_counts(path)
        for level, comps in expected.items():
            assert level in mapping, f"Level {level} missing in {fname}"
            for comp, count in comps.items():
                actual = mapping[level][comp]
                assert actual == count, f"{fname}: {level} {comp} expected {count}, got {actual}"

def test_build_counts(tmp_path: Path):
    from parse_nas_log_levels import build_level_component_counts, to_rows
    content = '\n'.join([
        '2024-10-25 01:44:23.024 +00:00 [Verbose    ] [NAS ] [T=12,ET=] msg',
        '2024-10-25 01:44:23.024 +00:00 [Verbose    ] [SCF ] [T=12,ET=] msg',
        '2024-10-25 01:44:23.024 +00:00 [Error      ] [NAS ] [T=12,ET=] msg',
    ])
    p = tmp_path / "sample.log"
    p.write_text(content)
    mapping = build_level_component_counts(p)
    rows = to_rows(mapping)
    # Expect 3 rows after flatten, counts include 2 Verbs (NAS+SCF) and 1 Error (NAS)
    assert set((lvl, comp) for lvl, comp, _ in rows) == {("Verbose", "NAS"), ("Verbose", "SCF"), ("Error", "NAS")}


test_nas_log_level_component_counts()