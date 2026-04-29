# msgbusviz (Python client)

Python client SDK for msgbusviz. Connects to the sidecar over WebSocket and pushes message events.

```python
from msgbusviz import Client

client = Client(url="ws://localhost:8080/ws")
client.connect()
client.send_message("orders", from_="OrderService", to="InventoryService")
client.close()
```

See the main project at https://github.com/example/msgbusviz for details.
