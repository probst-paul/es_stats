from __future__ import annotations

import psycopg

from es_stats.cli.main import build_parser, import_csv_contract_only


def test_import_end_to_end_imports_1m_and_rebuilds_30m(
    tmp_path,
    monkeypatch,
    postgres_url: str,
    pg_conn: psycopg.Connection,
):
    monkeypatch.setenv("ES_STATS_DATABASE_URL", postgres_url)

    csv_path = tmp_path / "bars.csv"
    lines = ["datetime,open,high,low,last,volume,# of Trades\n"]
    start_hour, start_min = 8, 30
    for i in range(32):
        total_min = start_min + i
        hh = start_hour + (total_min // 60)
        mm = total_min % 60
        dt = f"2025-01-01 {hh:02d}:{mm:02d}"

        o = 100.0 + i
        h = o + 1.0
        l = o - 1.0
        c = o + 0.5

        lines.append(f"{dt},{o},{h},{l},{c},1,1\n")

    csv_path.write_text("".join(lines))

    parser = build_parser()
    args = parser.parse_args(
        [
            "import-csv",
            "--file",
            str(csv_path),
            "--symbol",
            "ES",
            "--timezone",
            "America/Chicago",
            "--merge-policy",
            "skip",
        ]
    )

    rc = import_csv_contract_only(args, parser)
    assert rc == 0

    with psycopg.connect(postgres_url) as conn:
        imp = conn.execute(
            """
            SELECT import_id, status, row_count_read, row_count_inserted, row_count_updated
            FROM imports
            ORDER BY import_id DESC
            LIMIT 1;
            """
        ).fetchone()

        assert imp is not None
        assert imp[1] == "success"
        assert imp[2] == 32
        assert imp[3] == 32
        assert imp[4] == 0

        c1m = conn.execute("SELECT COUNT(*) FROM bars_1m;").fetchone()[0]
        assert c1m == 32

        c30m = conn.execute("SELECT COUNT(*) FROM bars_30m;").fetchone()[0]
        assert c30m == 2

        r_0830 = conn.execute(
            """
            SELECT bucket_ct_minute_of_day, bar_count_1m, is_complete,
                   open, high, low, close, volume, trades_count,
                   session, period_index
            FROM bars_30m
            WHERE bucket_ct_minute_of_day = 510;
            """
        ).fetchone()

        assert r_0830 is not None
        assert int(r_0830[1]) == 30
        assert int(r_0830[2]) == 1
        assert float(r_0830[3]) == 100.0
        assert float(r_0830[6]) == 129.5
        assert float(r_0830[4]) == 130.0
        assert float(r_0830[5]) == 99.0
        assert int(r_0830[7]) == 30
        assert int(r_0830[8]) == 30
        assert r_0830[9] == "RTH"
        assert int(r_0830[10]) == 0

        r_0900 = conn.execute(
            """
            SELECT bucket_ct_minute_of_day, bar_count_1m, is_complete,
                   open, high, low, close, volume, trades_count,
                   session, period_index
            FROM bars_30m
            WHERE bucket_ct_minute_of_day = 540;
            """
        ).fetchone()

        assert r_0900 is not None
        assert int(r_0900[1]) == 2
        assert int(r_0900[2]) == 0
        assert float(r_0900[3]) == 130.0
        assert float(r_0900[6]) == 131.5
        assert float(r_0900[4]) == 132.0
        assert float(r_0900[5]) == 129.0
        assert int(r_0900[7]) == 2
        assert int(r_0900[8]) == 2
        assert r_0900[9] == "RTH"
        assert int(r_0900[10]) == 1
