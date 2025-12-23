import pandas as pd

def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        order_id=df["order_id"].astype("string"),
        user_id=df["user_id"].astype("string"),
        amount=pd.to_numeric(df["amount"], errors="coerce").astype("Float64"),
        quantity=pd.to_numeric(df["quantity"], errors="coerce").astype("Int64"),
    )


def missingness_report(df):
    return (df.isna().sum().rename("n_missing").to_frame().assign(missing = lambda t: t["n_missing"] / len(df)).sort_values("missing", ascending = False))


def add_missing_flags(df, cols):
    df_copy = df.copy()
    for i in cols:
        df_copy[f"{i}__isna"] = df_copy[i].isna()
    return df_copy



def normalize_text(s: pd.Series):
    return (
        s.astype("string").str.strip().str.casefold().str.split().str.join(" "))


def apply_mapping(s: pd.Series, mapping: dict):
    return s.map(lambda x: mapping.get(x, x))



def dedupe_keep_latest(df: pd.DataFrame, key_cols: list[str], timeStep_col: str):
    return (
        df.sort_values(timeStep_col)
          .drop_duplicates(subset=key_cols, keep="last")
          .reset_index(drop=True)
    )
