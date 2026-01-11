import pandas as pd

# Path to your pickle file (update this)
pkl_path = r"G:\My Drive\Trading\Historical_Options_Data\BANKNIFTY_20251125_minute.pkl"
pd.set_option("display.max_columns", None)     # show all columns
pd.set_option("display.width", None)           # no fixed line width
pd.set_option("display.max_colwidth", None)    # don't truncate long text cells
pd.set_option("display.expand_frame_repr", False)  # avoid wrapping into multiple lines
# Load (works for a pickled DataFrame, or anything pickled via pandas)
obj = pd.read_pickle(pkl_path)

print("Type:", type(obj))

# If it's a DataFrame
if isinstance(obj, pd.DataFrame):
    print("Shape:", obj.shape)
    print("Columns:", obj.columns.tolist())
    print("\n--- head() ---")
    print(obj.head(10))
    print("\n--- sample() ---")
    print(obj.sample(n=min(10, len(obj)), random_state=42))

# If it's a Series
elif isinstance(obj, pd.Series):
    print("Length:", len(obj))
    print("\n--- head() ---")
    print(obj.head(10))
    print("\n--- sample() ---")
    print(obj.sample(n=min(10, len(obj)), random_state=42))

# If it's something else (e.g., dict of DataFrames)
elif isinstance(obj, dict):
    print("Dict keys (first 20):", list(obj.keys())[:20])
    # Try showing a sample of the first key's value
    first_key = next(iter(obj))
    v = obj[first_key]
    print("\nFirst key:", first_key, "-> type:", type(v))
    if isinstance(v, pd.DataFrame):
        print("Shape:", v.shape)
        print(v.head(10))
    else:
        print(v)

else:
    # Fallback: just print a preview
    print(obj)
