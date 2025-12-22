# Check if there is col not in the dataFrame
def require_columns(df, cols):
    Missing_columns = [miss for miss in cols if miss not in  df.columns]
    assert not Missing_columns, f"The missing columns is {Missing_columns}"

def assert_non_empty(df, name="df"):
    assert not df.empty, f"The Data ({name}) is an empty"