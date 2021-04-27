# main
from src.rdma_client import RdmaClient
from config.config import *

if __name__ == "__main__":
    c = RdmaClient(ADDR, PORT, NAME)
    c.request()
    c.close()
