import pandas as pd

def sma(series, periods):
        if len(series) < periods:
            return None
        return series.rolling(window=periods, min_periods=periods).mean()

def ema(series, periods):
    sma = series.rolling(window=periods, min_periods=periods).mean()[:periods]
    rest = series[periods:]
    return pd.concat([sma, rest]).ewm(span=periods, adjust=False).mean()


def rsi(close, n=14):
    """Relative Strength Index (RSI)
    Compares the magnitude of recent gains and losses over a specified time
    period to measure speed and change of price movements of a security. It is
    primarily used to attempt to identify overbought or oversold conditions in
    the trading of an asset.
    https://www.investopedia.com/terms/r/rsi.asp
    Args:
        close(pandas.Series): dataset 'Close' column.
        n(int): n period.
        fillna(bool): if True, fill nan values.
    Returns:
        pandas.Series: New feature generated.
    """
    diff = close.diff()
    which_dn = diff < 0

    up, dn = diff, diff*0
    up[which_dn], dn[which_dn] = 0, -up[which_dn]

    emaup = ema(up, n)
    emadn = ema(dn, n)

    rsi = 100 * emaup / (emaup + emadn)
    return pd.Series(rsi, name='rsi')

def new_rsi(close, n=14):
    # Get the difference in price from previous step
    delta = close.diff()
    # Get rid of the first row, which is NaN since it did not have a previous
    # row to calculate the differences
    delta = delta[1:]

    # Make the positive gains (up) and negative gains (down) Series
    up, down = delta.copy(), delta.copy()
    up[up < 0] = 0
    down[down > 0] = 0

    # Calculate the EWMA
    roll_up1 = ema(up, n)
    roll_down1 = ema(down.abs(), n)

    # Calculate the RSI based on EWMA
    RS1 = roll_up1 / roll_down1
    RSI1 = 100.0 - (100.0 / (1.0 + RS1))

    # Calculate the SMA
    roll_up2 = sma(up, n)
    roll_down2 = sma(down.abs(), n)

    # Calculate the RSI based on SMA
    RS2 = roll_up2 / roll_down2
    RSI2 = 100.0 - (100.0 / (1.0 + RS2))

    return RSI1, RSI2

def macd(close, n_fast=12, n_slow=26, fillna=False):
    """Moving Average Convergence Divergence (MACD)
    Is a trend-following momentum indicator that shows the relationship between
    two moving averages of prices.
    https://en.wikipedia.org/wiki/MACD
    Args:
        close(pandas.Series): dataset 'Close' column.
        n_fast(int): n period short-term.
        n_slow(int): n period long-term.
        fillna(bool): if True, fill nan values.
    Returns:
        pandas.Series: New feature generated.
    """
    emafast = ema(close, n_fast)
    emaslow = ema(close, n_slow)
    macd = emafast - emaslow

    return pd.Series(macd, name='MACD_%d_%d' % (n_fast, n_slow))

def RSI(df, n):
    """
    Relative Strength Index
    """
    i = 0
    UpI = [0]
    DoI = [0]
    while i + 1 <= len(df) - 1:  # df.index[-1]
        UpMove = df.iat[i + 1, df.columns.get_loc('high')] - df.iat[i, df.columns.get_loc('high')]
        DoMove = df.iat[i, df.columns.get_loc('low')] - df.iat[i + 1, df.columns.get_loc('low')]
        if UpMove > DoMove and UpMove > 0:
            UpD = UpMove
        else:
            UpD = 0
        UpI.append(UpD)
        if DoMove > UpMove and DoMove > 0:
            DoD = DoMove
        else:
            DoD = 0
        DoI.append(DoD)
        i = i + 1
    UpI = pd.Series(UpI)
    DoI = pd.Series(DoI)
    PosDI = pd.Series(UpI.ewm(span=n, min_periods=n - 1).mean())
    NegDI = pd.Series(DoI.ewm(span=n, min_periods=n - 1).mean())
    result = pd.Series(PosDI / (PosDI + NegDI), name='RSI_' + str(n))
    return result

def calculate_rsi(stock_data, n):
    stock_data_subset = stock_data[-n:]

    gain_count=0
    loss_count=0

    for i in range(1,len(stock_data_subset)):
        if((stock_data_subset[i]['close']-stock_data_subset[i-1]['close'])>0):
          gain_count=gain_count+stock_data_subset[i]['high']-stock_data_subset[i-1]['close']
        if ((stock_data_subset[i]['close']-stock_data_subset[i-1]['close'])<0):
           loss_count=loss_count+stock_data_subset[i-1]['close']-stock_data_subset[i]['close']

    return 100 - (100 / (1 + (gain_count / loss_count)))
