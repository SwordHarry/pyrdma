# main
from src.rdma.socket.rdma_socket_client import RdmaSocketClient
from src.config.config import ADDR, PORT_STR
from src.socket.client import SocketClient

if __name__ == "__main__":
    # c = RdmaClient(ADDR, PORT_STR)
    # c.request()
    # c.close()
    c = SocketClient()
    # c.request()
    c.push_file("./test/push/src/50M.file")
    # c.pull_file("./test/pull/des/50M.file")
    # c = RdmaSocketClient(ADDR, PORT_STR)
    # c.request()
