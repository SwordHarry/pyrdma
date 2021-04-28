# rdma server
from pyverbs.qp import QPInitAttr, QPCap
from pyverbs.cmid import CMID, AddrInfo, CMEventChannel, CMEvent
import pyverbs.cm_enums as ce
from pyverbs.mem_alloc import malloc
import copy


class RdmaServer:
    # sockaddr_in addr
    # rdma_cm_event event
    # rdma_cm_id listener
    # rdma_event_channel ec
    # port
    def __init__(self, addr, port, name, max_recv_wr=1):
        cap = QPCap(max_recv_wr=max_recv_wr)
        qp_init_attr = QPInitAttr(cap=cap)
        self.addr_info = AddrInfo(src=addr, service=port, port_space=ce.RDMA_PS_TCP, flags=ce.RAI_PASSIVE)
        self.event_channel = CMEventChannel()
        self.listener = CMID(creator=self.event_channel, qp_init_attr=qp_init_attr)
        self.event = None  # event deep copy
        print("ready to listen on ", addr + ":" + port)

    def run(self):
        self.listener.bind_addr(self.addr_info)
        self.listener.listen(backlog=10)
        print("listening... ")
        # new_id = self.listener.get_request()
        # new_id.accept()
        while True:
            # block until the event come
            event = CMEvent(self.event_channel)
            print(event.event_type)
            # self.event = copy.deepcopy(event)
            event.ack_cm_event()
            # self.on_event()

    def on_event(self):
        print(self.event.event_type)
        if self.event.event_type == ce.RDMA_CM_EVENT_CONNECT_REQUEST:
            print("connected")

    def on_connection(self):
        pass

    def close(self):
        self.listener.close()
        self.addr_info.close()
