import pandas as pd
import pathlib as Path

# missin values we may face
missing = ["","NA","N/A","null","None"]


# read the order CSV and make sure some data type
def read_order_csv(input_path: Path) -> pd.DataFrame:
    Csv = pd.read_csv(input_path,
                      dtype={"order_id":"string","user_id":"string"},
                      na_values=missing)
    return Csv

# read the user CSV and make sure some data type
def read_user_csv(input_path: Path) -> pd.DataFrame:
    Csv = pd.read_csv(input_path,dtype={"user_id": "string"},
                      na_values=missing)

    return Csv

# Chosing the outputPath and save the csv as "parquet"
def Write_parquet(df, output_path : Path):
    output_path.Path.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)

# read the "parquet" 
def read_parquet(input_path):
    ParQ = pd.read_parquet(input_path)
    return ParQ

# i wrote all the coment by my self dont worry 
