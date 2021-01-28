from upstox_api.api import *

import Utils as util

s = Session (util.UPSTOX_API_KEY)
s.set_redirect_uri ('http://edueasy.in')
s.set_api_secret ('tjpk0m91f5')
# https://api.upstox.com/index/dialog/authorize?apiKey=5LfPWD6ZJh8MqTeHvikvU6USVnK6w1uk7imllV2z&redirect_uri=http%3A%2F%2Fedueasy.in&response_type=code
# print (s.get_login_url())
s.set_code ('bcea90c00cc6a8c67c91e9213712d6f9c191c5d8')
access_token = s.retrieve_access_token()
print ('Received access_token: %s' % access_token)
open (util.UPSTOX_LATEST_ACCESS_CODE_FILE, 'w+').close ()
with open (util.UPSTOX_LATEST_ACCESS_CODE_FILE, 'a') as the_file:
    the_file.write (access_token)
