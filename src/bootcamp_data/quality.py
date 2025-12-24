import pandas as pd
# Check if there is columns not in the dataFrame
def require_columns(df, cols):
    Missing_columns = [miss for miss in cols if miss not in  df.columns]
    assert not Missing_columns, f"The missing columns is {Missing_columns}"



# Check if the DataFrame is empty
def assert_non_empty(df, name="df"):
    assert not df.empty, f"The Data ({name}) is an empty"



# Check is it Unique or duplicate in the KEY column 
def assert_unique_key(df,key,allow_na = False):
    if not allow_na:
        assert df[key].notna().all(),f"The column {key} has NAN values"
    duplcate = df[key].duplicated(keep = False) & df[key].notna()
    assert not duplcate.all(), f"The {key} column is not unique and {duplcate.sum()} duplicate rows"



# Check that the series values are within range
def assert_in_range( seriess , Lowerr = None , Higher = None , name = "value"):
    x = seriess.dropna()
    if Lowerr is not None:
        assert (x >= Lowerr).all(),f" The {name} are below the {Lowerr}" 
    if Higher is not None:
        assert (x <= Lowerr).all(),f" The {name} are above the {Higher}" 



# All the coments For OSAMA ALGHAMDI