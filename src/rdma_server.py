# rdma server
from pyverbs.device import Context
from pyverbs.qp import QPInitAttr, QPCap
from pyverbs.cmid import CMID, AddrInfo, CMEventChannel, CMEvent
import pyverbs.cm_enums as ce

from common import GlobalContext

con_req_ctx = None


class RdmaServer:
    # sockaddr_in addr
    # rdma_cm_event event
    # rdma_cm_id listener
    # rdma_event_channel ec
    # port
    def __init__(self, addr, port, name, options):
        self.options = options
        self.addr_info = AddrInfo(src=addr, service=port, port_space=ce.RDMA_PS_TCP, flags=ce.RAI_PASSIVE)
        self.event_channel = CMEventChannel()
        self.listener = CMID(creator=self.event_channel)
        self.ctx = Context(name=name)
        self.event = None  # event deep copy
        print("ready to listen on ", addr + ":" + port)

        # event loop map config
        self.event_map = {
            ce.RDMA_CM_EVENT_CONNECT_REQUEST: self._on_connect_request,

        }

    def run(self):
        self.listener.bind_addr(self.addr_info)
        self.listener.listen(backlog=10)
        print("listening... ")
        # new_id = self.listener.get_request()
        # new_id.accept()
        while True:
            # block until the event come
            self.event = CMEvent(self.event_channel)
            print(self.event.event_type, self.event.event_str())
            self.event_map[self.event.event_type]()
            self.event.ack_cm_event()

    def _on_connect_request(self):
        print("received connection request")
        # build_context
        global con_req_ctx
        con_req_ctx = GlobalContext(context=self.listener)
        # poll_cq

        # build_qp_attr
        qp_options = self.options.get("qp_init")
        cap = QPCap(max_send_wr=qp_options.get("max_send_wr"), max_recv_wr=qp_options.get("max_recv_wr"),
                    max_send_sge=qp_options.get("max_send_sge"), max_recv_sge=qp_options.get("max_recv_sge"))
        self.qp_init_attr = QPInitAttr(qp_type=qp_options.get("qp_type"), cap=cap, scq=con_req_ctx.cq,
                                       rcq=con_req_ctx.cq)


    def close(self):
        self.listener.close()
        self.addr_info.close()
