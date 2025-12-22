# Chick if there is col not in the dataFrame
def require_columns(df, cols):
    missing = [item for item in df]
    return [miss for miss in missing if miss not in cols]

def assert_non_empty(df, name="df"):
    return