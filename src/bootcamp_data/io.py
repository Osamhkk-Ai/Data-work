import pandas as pd
import pathlib as Path

missing = ["","NA","N/A","null","None"]

def read_order_csv(input_path: Path) -> pd.DataFrame:
    Csv = pd.read_csv(input_path,
                      dtype={"order_id":"string","user_id":"string"},
                      na_values=missing)
    return Csv


def read_user_csv(input_path: Path) -> pd.DataFrame:
    Csv = pd.read_csv(input_path,dtype={"user_id": "string"},
                      na_values=missing)

    return Csv


def Write_parquet(df, output_path : Path):
    output_path.Path.mkdir(parents=True, exist_ok=True)
    
    return

def read_parquet(input_path):
    ParQ = pd.read_parquet(input_path)
    return ParQ
