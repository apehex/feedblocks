import json
import os

import requests
import web3

# TARGETS #####################################################################

# SOURCE CODE #################################################################

ETH_API_KEY = os.environ.get('ETH_API_KEY', '')
ETH_API_URL = 'https://api.etherscan.io/api?module=contract&action=getsourcecode&address={address}&apikey={key}'

__a = '0x2ea46070dCD7C7DDD2114b4508548551d8630471'
__r = requests.get(ETH_API_URL.format(address=__a, key=ETH_API_KEY))
__t = __r.text

# RUNTIME BYTECODE ############################################################

# CREATION BYTECODE ###########################################################
