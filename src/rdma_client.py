# rdma client
from pyverbs.device import Context
from pyverbs.qp import QPInitAttr, QPCap
from pyverbs.pd import PD
from pyverbs.cmid import CMID, AddrInfo, CMEventChannel, CMEvent
import pyverbs.cm_enums as ce
from config.config import *
from src.common import AddrResolvedContext, PollThread, die

addr_res_ctx = None


class RdmaClient:
    def __init__(self, addr, port, name, max_recv_wr=1):
        cap = QPCap(max_recv_wr=max_recv_wr)
        self.qp_init_attr = QPInitAttr(cap=cap)
        # src=addr, ,flags=ce.RAI_PASSIVE
        self.addr_info = AddrInfo(dst=addr, service=port, port_space=ce.RDMA_PS_TCP)
        # event_channel, cmid and event
        self.event_channel = CMEventChannel()
        self.cid = CMID(creator=self.event_channel)
        self.ctx = Context(name=name)
        self.event = None

        # poll cq
        self.poll_t = PollThread(task=)
        # event loop map config
        self.event_map = {
            ce.RDMA_CM_EVENT_ADDR_RESOLVED: self._on_addr_resolved,
        }

    def request(self):
        self.cid.resolve_addr(self.addr_info, TIMEOUT_IN_MS)
        # self.cid.resolve_route()
        # self.cid.connect()
        while True:
            self.event = CMEvent(self.event_channel)
            print(self.event.event_type, self.event.event_str(), ce.RDMA_CM_EVENT_ADDR_RESOLVED)
            # need to copy the event and then ack the event
            # TODO: how to copy the event
            self.event_map[self.event.event_type]()
            self.event.ack_cm_event()

    def close(self):
        self.cid.close()
        self.addr_info.close()

    # resolved addr
    def _on_addr_resolved(self):
        print("address resolved.")
        global addr_res_ctx
        if addr_res_ctx is not None:
            if addr_res_ctx.ctx != self.ctx:
                die("cannot handle events in more than one context.")
            return

        addr_res_ctx = AddrResolvedContext(context=self.ctx)
        # poll_cq

    def _poll_cq(self):
