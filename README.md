# Package User Zone

## Installation

```bash
pip install python-ftx
```

## Sample Code
### Restful Api
```python
from ftx import Client
import os

API = os.getenv("API")
SECRET = os.getenv("SECRET")

client = Client(API, SECRET)
info = client.get_markets()
print(info)

```
### Websocket

```python
from ftx import ThreadedWebsocketManager

def on_read(payload):
    print(payload)

API = os.getenv("API")
SECRET = os.getenv("SECRET")

wsm = ThreadedWebsocketManager(API, SECRET)
wsm.start()

# Un-auth subscribe
name = 'market_connection'
wsm.start_socket(on_read, socket_name=name)
wsm.subscribe(name, channel="ticker", op="subscribe", market="BTC/USDT")

# Auth subscribe
name = 'private_connection'
wsm.start_socket(on_read, conn_name=name)
wsm.login(conn_name=name)
wsm.subscribe(
    name,
    channel="fills",
    op="subscribe",
)
```

### Subaccount
```
clinet = Client(API, SECRET, "Subaacount_name")
wsm = ThreadedWebsocketManager(API, SECRET, "Subaccount_name")
```

# Developer Zone

## Lint

```bash
$ make lint
```

## Donation
**I put dontaion address here because all open source Crypto API did. It seems unprofessional if I don't**

ETH:0xB32A3CbEaD5667e026CCEC7118b132DCA349A8e6
