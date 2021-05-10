# main
from src.rdma.rdma_server import RdmaServer
from src.socket_server import SocketServer
from src.config.config import ADDR, PORT_STR, NAME, OPTIONS

if __name__ == "__main__":
    s = RdmaServer(ADDR, PORT_STR, NAME, OPTIONS)
    s.run()
    s.close()
    # s = SocketServer()
    # s.serve()
