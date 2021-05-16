# main
from src.rdma.socket.rdma_socket_server import RdmaSocketServer
from src.config.config import ADDR, PORT_STR, OPTIONS
from src.socket.server import SocketServer

if __name__ == "__main__":
    # s = RdmaServer(ADDR, PORT_STR, OPTIONS)
    # s.run()
    # s.close()
    # s = SocketServer()
    # s.serve()
    s = RdmaSocketServer(ADDR, PORT_STR, OPTIONS)
    s.serve()
