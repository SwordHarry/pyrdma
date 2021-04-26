# main
from common import Context, Connection
from rdma_server import RdmaServer
from config.config import *

if __name__ == "__main__":
    s = RdmaServer(ADDR, PORT)
    s.run()
    s.close()
