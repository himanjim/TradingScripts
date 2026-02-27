import pandas as pd
df = pd.read_pickle(r"C:\Users\Local User\Documents\dhan_expired_rolling_pickles\NIFTY_W_EXP1_ATMpm10_20260210_20260217.pkl")
chk = df.groupby("target_expiry_date")["date_ist"].max()
print((chk == chk.index).value_counts())
print(chk.head(10))