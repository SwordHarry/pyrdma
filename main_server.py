# main
from src.rdma_server import RdmaServer
from config.config import *

if __name__ == "__main__":
    s = RdmaServer(ADDR, PORT, NAME)
    s.run()
    s.close()
