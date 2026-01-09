from __future__ import annotations

from pathlib import Path

import pytest

from es_stats.cli.main import build_parser, import_csv_contract_only


def test_import_csv_help_exits_zero():
    parser = build_parser()
    with pytest.raises(SystemExit) as exc:
        parser.parse_args(["import-csv", "--help"])
    assert exc.value.code == 0


def test_import_csv_rejects_missing_file(tmp_path: Path):
    parser = build_parser()
    args = parser.parse_args(
        ["import-csv", "--file", str(tmp_path / "missing.csv"), "--symbol", "ES"])
    with pytest.raises(SystemExit) as exc:
        import_csv_contract_only(args, parser)
    # argparse uses exit code 2 for "parser.error(...)"
    assert exc.value.code == 2


def test_import_csv_accepts_valid_args(tmp_path: Path):
    f = tmp_path / "bars.csv"
    f.write_text(
        "datetime,open,high,low,last,volume\n"
        "2025-01-01 08:30,100,101,99,100.5,10\n"
    )

    parser = build_parser()
    args = parser.parse_args(
        ["import-csv", "--file", str(f), "--symbol", "ES", "--timezone",
         "America/Chicago", "--merge-policy", "skip"]
    )

    rc = import_csv_contract_only(args, parser)
    assert rc == 0


def test_import_csv_rejects_invalid_timezone(tmp_path: Path):
    f = tmp_path / "bars.csv"
    f.write_text("x\n")

    parser = build_parser()
    args = parser.parse_args(
        ["import-csv", "--file", str(f), "--symbol", "ES", "--timezone", "Not/AZone"])

    with pytest.raises(SystemExit) as exc:
        import_csv_contract_only(args, parser)
    assert exc.value.code == 2
