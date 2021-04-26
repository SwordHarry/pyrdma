# rdma server
from pyverbs.qp import QPInitAttr, QPCap
from pyverbs.cmid import CMID, AddrInfo
import pyverbs.cm_enums as ce


class RdmaServer:
    # sockaddr_in addr
    # rdma_cm_event event
    # rdma_cm_id listener
    # rdma_event_channel ec
    # port
    def __init__(self, addr, port, max_recv_wr=1):
        cap = QPCap(max_recv_wr=1)
        qp_init_attr = QPInitAttr(cap=cap)
        sai = AddrInfo(src=addr, service=port, port_space=ce.RDMA_PS_TCP, flags=ce.RAI_PASSIVE)
        # sai = AddrInfo(addr, service, port, None, ce.RDMA_PS_TCP, ce.RAI_PASSIVE)
        self.listener = CMID(creator=sai, qp_init_attr=qp_init_attr)
        print("ready to listen on ", addr + ":" + port)

    def run(self):
        # self.listener.bind_addr()
        self.listener.listen(backlog=10)
        while True:
            new_id = self.listener.get_request()  # sync
            new_id.accept()
            print(new_id.event_channel)


    def on_connection(self):
        pass

    def close(self):
        self.listener.close()
