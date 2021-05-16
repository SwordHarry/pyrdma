# main
from src.rdma.socket.rdma_socket_client import RdmaSocketClient
from src.config.config import ADDR, PORT_STR
from src.socket.client import SocketClient

if __name__ == "__main__":
    # c = RdmaClient(ADDR, PORT_STR)
    # c.request()
    # c.close()
    # c = SocketClient()
    # c.request()
    c = RdmaSocketClient(ADDR, PORT_STR)
    c.request()
