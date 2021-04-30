# main
from src.rdma_server import RdmaServer
from src.config.config import *

if __name__ == "__main__":
    s = RdmaServer(ADDR, PORT, NAME, OPTIONS)
    s.run()
    s.close()
