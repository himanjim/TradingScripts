import Utils as util
from kiteconnect import KiteConnect

kite = KiteConnect (api_key=util.KITE_API_KEY)

# https://kite.trade/connect/login?api_key=453dipfh64qcl484&v=3
access_token = kite.generate_session ('g9xM3TNm0n6PV2zqymdo3z0wQxKNvrC8', util.KITE_API_SECRET)["access_token"]
print ('Received access_token: %s' % access_token)
open (util.KITE_LATEST_ACCESS_CODE_FILE, 'w+').close ()
with open (util.KITE_LATEST_ACCESS_CODE_FILE, 'a') as the_file:
    the_file.write (access_token)
