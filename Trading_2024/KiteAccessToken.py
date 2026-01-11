import OptionTradeUtils as oUtils
from kiteconnect import KiteConnect

kite = KiteConnect (api_key=oUtils.KITE_API_KEY)

# https://kite.trade/connect/login?api_key=453dipfh64qcl484&v=3
access_token = kite.generate_session ('wXfh14A4HuNjiHn2WAsJDZljC7bYPkiN', oUtils.KITE_API_SECRET)["access_token"]
print ('Received access_token: %s' % access_token)
# open (oUtils.KITE_LATEST_ACCESS_CODE_FILE, 'w+').close ()
# with open (oUtils.KITE_LATEST_ACCESS_CODE_FILE, 'a') as the_file:
#     the_file.write (access_token)
