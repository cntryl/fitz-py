from fitz_py import Client, ClientConfig, ConnectionState


def test_client_starts_disconnected() -> None:
    client = Client(ClientConfig(url="ws://localhost:4190/ws"))
    assert client.state is ConnectionState.DISCONNECTED
