from web3 import Web3
from typing import Any
import statistics
from datetime import datetime

started_at = datetime.now()
url: str = 'https://mainnet.infura.io/v3/c2835fbd1c6c4b0d9f48ea78c1af395f'
w3 = Web3(Web3.HTTPProvider(url))
ether = 1000000000000000000 # ether conversion value

is_connected: bool = w3.isConnected()
block_number: int = w3.eth.block_number
last_block: Any = w3.eth.get_block('latest')
last_transactions: list = last_block['transactions']

print('url', url)
print('is_connected:', is_connected)
print('block_number:', block_number)

transaction_values_eth = []   # list of all transaction values in ETH
transaction_gas_eth = []      # list of all gas values in ETH
transaction_gasprice_eth = [] # list of all gasprice values in ETH

for transaction_data in last_transactions:
  transaction = w3.eth.getTransaction(transaction_data)

  # transaction params
  # transaction_hash = transaction['hash']
  # transaction_from = transaction['from']
  # transaction_to = transaction['to']
  transaction_gas = transaction['gas']
  transaction_value = transaction['value']
  transaction_gasprice = transaction['gasPrice']
  # transaction_index = transaction['transactionIndex']

  # calculation, conversion to ETH
  transaction_values_eth.append(transaction_value / ether)
  transaction_gas_eth.append(transaction_gas / ether)
  transaction_gasprice_eth.append(transaction_gasprice / ether)
  # break

finished_at = datetime.now()
transaction_count = len(transaction_values_eth)
print('Block calculated in %s seconds.' % (finished_at - started_at))
print('Count:', transaction_count)
print('Transaction values:')
print('Maximal value [ETH]:', max(transaction_values_eth))
print('Minimal value [ETH]:', min(transaction_values_eth))
print('Median value [ETH]:', statistics.median(transaction_values_eth))
print('Average value [ETH]:', sum(transaction_values_eth) / transaction_count)
print('')
print('Gas values:')
print('Maximal value [ETH]:', max(transaction_gas_eth))
print('Minimal value [ETH]:', min(transaction_gas_eth))
print('Median value [ETH]:', statistics.median(transaction_gas_eth))
print('Average value [ETH]:', sum(transaction_gas_eth) / transaction_count)
print('')
print('GasPrice values:')
print('Maximal value [ETH]:', max(transaction_gasprice_eth))
print('Minimal value [ETH]:', min(transaction_gasprice_eth))
print('Median value [ETH]:', statistics.median(transaction_gasprice_eth))
print('Average value [ETH]:', sum(transaction_gasprice_eth) / transaction_count)
print(' --- ')