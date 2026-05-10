import pandas as pd

PICKLE_FILE = r"C:\Users\himan\Documents\Audacity.pkl"

df = pd.read_pickle(PICKLE_FILE)

print(df.head(10).to_string(index=False))