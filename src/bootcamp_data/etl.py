from __future__ import annotations
from pathlib import Path
import pandas as pd
from bootcamp_data.io import read_orders_csv, read_users_csv
from bootcamp_data.quality import require_columns, assert_non_empty, assert_unique_key
from bootcamp_data.transforms import enforce_schema,add_missing_flags,normalize_text, apply_mapping,parse_datetime,add_time_parts,winsorize
from bootcamp_data.joins import safe_left_join
from bootcamp_data.io import write_parquet
import json
import logging
from dataclasses import dataclass, asdict


log = logging.getLogger(__name__)




@dataclass(frozen=True)
class ETLConfig:
    root: Path
    raw_orders: Path
    raw_users: Path
    out_orders_clean: Path
    out_users: Path
    out_analytics: Path
    run_meta: Path


def load_inputs(cfg: ETLConfig):
    orders = read_orders_csv(cfg.raw_orders)
    users = read_users_csv(cfg.raw_users)
    return orders, users


def transform(orders_raw: pd.DataFrame, users: pd.DataFrame):
    # Check the correct columns is there.  wallah ana osama not ai 
    require_columns(orders_raw, ["order_id", "user_id", "amount", "quantity", "created_at", "status"])
    require_columns(users, ["user_id", "country", "signup_date"])
    
    # Check there is no empty and unique . again am osama wallah
    assert_non_empty(orders_raw, "orders_raw")
    assert_non_empty(users, "users")
    assert_unique_key(users, "user_id")



    # HERE we goint to enforec the data to become the right dtype
    orders = enforce_schema(orders_raw)

    orders_clean = orders.copy()


    # Now we gonna normalize the Word in the cell to make it all the same 
    status_norm = normalize_text(orders["status"])
    mapping = {
        "paid": "paid",
        "refund": "refund",
        "refunded": "refund",
    }
    orders = orders.assign(status_clean=apply_mapping(status_norm, mapping))



    # Here we just show the missing in new col for spicfic col
    orders = orders.pipe(add_missing_flags, cols=["amount", "quantity"])


    # Here is pipline for change the Date col from object to datatime and make year day month hour 
    orders = (
        orders
        .pipe(parse_datetime, col="created_at", utc=True)
        .pipe(add_time_parts, ts_col="created_at")
    )


    # Now we will join the table using the user_id
    joined = safe_left_join(
        orders,
        users,
        on="user_id",
        validate="many_to_one",
        suffixes=("", "_user"),
    )

    # this assert  will tell us if the join was good with no  extra rows or col or not 
    assert len(joined) == len(orders), "Row count changed after join (join explosion?)"


    # thsi one will delet any outlier from the dataFrame
    joined = joined.assign(amount_winsor=winsorize(joined["amount"]))


    return orders_clean, joined


def load_outputs(orders_clean : pd.DataFrame ,analytics: pd.DataFrame, users: pd.DataFrame, cfg: ETLConfig) -> None:
    write_parquet(orders_clean, cfg.out_orders_clean)
    write_parquet(users, cfg.out_users)
    write_parquet(analytics, cfg.out_analytics)


def write_run_meta(cfg: ETLConfig, *, analytics: pd.DataFrame) -> None:
    missing_created_at = int(analytics["created_at"].isna().sum())
    country_match_rate = 1.0 - float(analytics["country"].isna().mean())

    meta = {
        "rows_out": int(len(analytics)),
        "missing_created_at": missing_created_at,
        "country_match_rate": country_match_rate,
        "config": {k: str(v) for k, v in asdict(cfg).items()},
    }

    cfg.run_meta.parent.mkdir(parents=True, exist_ok=True)
    cfg.run_meta.write_text(json.dumps(meta, indent=2), encoding="utf-8")



def run_etl(cfg: ETLConfig) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    log.info("Loading inputs")
    orders_raw, users = load_inputs(cfg)

    log.info("Transforming (orders=%s, users=%s)", len(orders_raw), len(users))
    orders_clean, analytics = transform(orders_raw, users)

    log.info("Writing outputs")
    load_outputs(orders_clean, analytics, users, cfg)

    log.info("Writing run metadata")
    write_run_meta(cfg, analytics=analytics)