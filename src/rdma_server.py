# rdma server
# config
import src.config.config as c
import pyverbs.cm_enums as ce
# common
from src.common.common import die
from src.common.node import Node
# pyverbs
from pyverbs.cmid import CMEvent, ConnParam


class RdmaServer(Node):
    # sockaddr_in addr
    # rdma_cm_event event
    # rdma_cm_id listener
    # rdma_event_channel ec
    # port
    def __init__(self, addr, port, name, options=c.OPTIONS):
        super().__init__(addr, port, name, server_flag=True, options=options)
        print("ready to listen on ", addr + ":" + port)

        # event loop map config
        self.event_map = {
            ce.RDMA_CM_EVENT_CONNECT_REQUEST: self._on_connect_request,

        }

    def run(self):
        self.cid.bind_addr(self.addr_info)
        self.cid.listen(backlog=10)
        print("listening... ")
        while True:
            # block until the event come
            self.event = CMEvent(self.event_channel)
            print(self.event.event_type, self.event.event_str())
            self.event_map[self.event.event_type]()
            self.event.ack_cm_event()

    def _on_connect_request(self):
        print("received connection request")
        if self.s_ctx is not None:
            if self.s_ctx.ctx != self.ctx:
                die("cannot handle events in more than one context.")
            return
        # build_context
        self.build_context()
        conn_param = ConnParam()
        self.cid.accept(conn_param)

    def close(self):
        self.cid.close()
        self.addr_info.close()
