from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from dags.spx_web_source import fetch_spx_export_records


def main() -> None:
    parser = argparse.ArgumentParser(description="Test SPX web export download locally.")
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--headed", action="store_true", help="Run browser in headed mode.")
    parser.add_argument("--keep-download", action="store_true", help="Keep downloaded export file.")
    parser.add_argument("--output", help="Optional JSON output path for normalized records.")
    args = parser.parse_args()

    records = fetch_spx_export_records(
        args.start_date,
        args.end_date,
        headless=not args.headed,
        keep_download=args.keep_download,
        output_dir=os.getenv("SPX_WEB_DOWNLOAD_DIR"),
    )

    print(f"Downloaded and normalized {len(records)} rows.")
    if records:
        print("Columns:", ", ".join(records[0].keys()))
        print("Sample:", json.dumps(records[0], ensure_ascii=False, indent=2))

    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Saved normalized records to {output_path}")


if __name__ == "__main__":
    main()
