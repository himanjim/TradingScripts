import pandas as pd

# Example DataFrame
data = {
    'TRADE DATE': ['2022-01-01', '2022-06-01', '2023-01-01', '2023-06-01'],
    'PRODUCT': ['A', 'B', 'A', 'B'],
    'MGD PROFIT': [100, 150, 200, 250],
    'SALES': [1000, 1200, 1300, 1400]
}

df = pd.DataFrame(data)
df['TRADE DATE'] = pd.to_datetime(df['TRADE DATE'])

# Group by year and product, and aggregate multiple columns
result = df.groupby([pd.Grouper(key='TRADE DATE', freq='Y'), 'PRODUCT']).agg({
    'MGD PROFIT': 'sum',
    'SALES': 'sum'
})

print(result)
