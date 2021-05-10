# main
from src.rdma.rdma_client import RdmaClient
from src.socket_client import SocketClient
from src.config.config import ADDR, PORT_STR, NAME

if __name__ == "__main__":
    c = RdmaClient(ADDR, PORT_STR, NAME)
    c.request()
    c.close()
    # c = SocketClient()
    # c.request()
