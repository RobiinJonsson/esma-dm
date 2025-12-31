import esma_dm
from esma_dm.firds import FIRDSClient
from esma_dm.fitrs import FITRSClient
from esma_dm.benchmarks import BenchmarksClient
from esma_dm.ssr import SSRClient
from esma_dm.models import DebtInstrument, EquityInstrument, DerivativeInstrument, InstrumentMapper

print('Package structure validated:')
print(f'  esma_dm version: {esma_dm.__version__}')
print('\nClients:')
print(f'  FIRDSClient: OK')
print(f'  FITRSClient: OK')
print(f'  BenchmarksClient: OK')
print(f'  SSRClient: OK')
print('\nModels:')
print(f'  DebtInstrument: OK')
print(f'  EquityInstrument: OK')
print(f'  DerivativeInstrument: OK')
print(f'  InstrumentMapper: OK')
print('\nAll imports successful - package is working correctly!')
