from decimal import Decimal
import pandas as pd

def get_expiration(options_ticker, my_date):
    path = '/data/thetadata/options'
    expirations = pd.read_csv(f"{path}/{options_ticker}/expirations.csv.gz", compression='gzip')
    expirations['date'] = pd.to_datetime(expirations['date'], format='%Y-%m-%d')
    expirations

    expiration_row = expirations[expirations['date'] == my_date]
    if expiration_row.empty:
        raise ValueError(f"Expiration not found for the given date: {my_date}")
    expiration = expiration_row.iloc[0]['date'].strftime('%Y%m%d')
    return expiration
 
def get_index_eod(ticker, date):
    year = date[:4]
    eod_file = f"/data/thetadata/indexes/{ticker}/eod/{year}.csv.gz"
    eods = pd.read_csv(eod_file, compression='gzip')

    # Convert 'date' column to string to ensure compatibility
    eods['date'] = eods['date'].astype(str)
    
    # Filter by date
    eod = eods[eods['date'] == date]
    
    # Convert 'date' column to datetime
    eod.loc[:, 'date'] = pd.to_datetime(eod['date'], format='%Y%m%d')

    return eod

def get_stock_close(ticker, interval, date, intervals):
    # Add SPY close at interval
    ohlc = pd.read_csv(f"/data/thetadata/stocks/{ticker}/{interval}/{date}.csv.gz", compression='gzip')
    for index, row in ohlc.iterrows():
        ms_of_day = row['ms_of_day']
        close_value = row['close']
        if ms_of_day in intervals:
            intervals[ms_of_day]['underlying_close'] = close_value
        # Optional: else clause to handle non-existent keys
    return intervals

def get_option_atm_strike(underlying_ticker, options_ticker, date):
    eod = get_index_eod(underlying_ticker, date)
    price = eod['open'].values[0] * 1000
    strikes = pd.read_csv(f"/data/thetadata/options/{options_ticker}/strikes/{date}.csv.gz", compression='gzip')
    atm_strike = strikes.loc[(strikes.sub(Decimal(str(price))).abs().idxmin())]
    atm_strike = atm_strike.iloc[0].strike
    print(f"atm strike: {atm_strike}")
    return atm_strike

def get_option_sd_strikes(underlying_ticker, options_ticker, date):
    atm_strike = get_option_atm_strike(underlying_ticker, options_ticker, date)

    range_value = 50 * 1000
    strikes = pd.read_csv(f"/data/thetadata/options/{options_ticker}/strikes/{date}.csv.gz", compression='gzip')
    strikes_within_range = strikes[(strikes['strike'] >= atm_strike - range_value) & (strikes['strike'] <= atm_strike + range_value)]
    return strikes_within_range

def get_option_quotes(ticker, date, strikes, interval):
    quotes = pd.read_csv(f"/data/thetadata/options/{ticker}/0dte/{interval}/{date}.csv.gz", compression='gzip')
    quotes = quotes[quotes['strike'].isin(strikes['strike'])]

    # Add columns
    quotes['mid'] = round((quotes['bid'] + quotes['ask']) / 2, 4)
    quotes['vbid'] = round((quotes['bid'] * quotes['bid_size']) /2, 4)
    quotes['vask'] =  round((quotes['ask'] * quotes['ask_size']) /2, 4)

    # Drop columns
    quotes.drop(columns=['expiration', 'root', 'bid_exchange', 'bid_condition', 'ask_exchange', 'ask_condition', 'date'], inplace=True)
    quotes.drop(columns=['bid', 'bid_size', 'ask', 'ask_size'], inplace=True)
    quotes.drop(columns=['Unnamed: 0'], inplace=True)
    # print(f"Number of quotes loaded: {len(quotes)}")
    return quotes


def interval_option_chains(quotes):
    intervals = {}
    grouped = quotes.groupby(['ms_of_day', 'right'])
    for (interval, right), group in grouped:
        chain = group[['strike', 'mid', 'vbid', 'vask']].set_index('strike').to_dict('index')
        if interval not in intervals:
            intervals[interval] = {'P': {}, 'C': {}}
        intervals[interval][right] = chain
    return intervals  # {ms : { P: strikes, C, strikes }}

def check(intervals):
   for interval in intervals.keys():
    if 'underlying_close' not in intervals[interval]:
        print(f"underlying_close value is missing for interval {interval}")   # expect last interval to be missing

   
def load(underlying_ticker, option_ticker, etf_ticker, date, interval):
    print(f"Loading {underlying_ticker} {option_ticker} {etf_ticker} on {date} with interval {interval}")
    strikes = get_option_sd_strikes(underlying_ticker, option_ticker, date)
    quotes = get_option_quotes(option_ticker, date, strikes, interval)
    intervals = interval_option_chains(quotes) # {ms : { P: strikes, C, strikes }}
    intervals = get_stock_close(etf_ticker, interval, date, intervals) # Add SPY close
    check(intervals)
    return intervals

