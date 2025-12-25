from pathlib import Path
from bootcamp_data.etl import ETLConfig, run_etl


ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    cfg = ETLConfig(
        root=ROOT,
        raw_orders=ROOT / "data" / "raw" / "orders.csv",
        raw_users=ROOT / "data" / "raw" / "users.csv",
        out_orders_clean=ROOT / "data" / "processed" / "orders_clean.parquet",
        out_users=ROOT / "data" / "processed" / "users.parquet",
        out_analytics=ROOT / "data" / "processed" / "analytics_table.parquet",
        run_meta=ROOT / "data" / "processed" / "_run_meta.json",
    )

    run_etl(cfg)


if __name__ == "__main__":
    main()