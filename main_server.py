# main
from src.socket_server import SocketServer

if __name__ == "__main__":
    # s = RdmaServer(ADDR, PORT, NAME, OPTIONS)
    # s.run()
    # s.close()
    s = SocketServer(name="rxe_0")
    s.serve()
