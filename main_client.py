# main
from src.socket_client import SocketClient

if __name__ == "__main__":
    # c = RdmaClient(ADDR, PORT, NAME)
    # c.request()
    # c.close()
    c = SocketClient(name="rxe_0")
    c.request()
