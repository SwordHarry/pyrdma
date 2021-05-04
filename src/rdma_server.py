# rdma server
# config
import src.config.config as c
import pyverbs.cm_enums as ce
import pyverbs.enums as e
# common
from src.common.common import die
from src.common.node import Node
# pyverbs
from pyverbs.cmid import CMID, CMEvent, ConnParam


def _server_on_completion(wc):
    if wc.status != e.IBV_WC_SUCCESS:
        die("_server_on_completion: status is not IBV_WC_SUCCESS")
    if wc.opcode & e.IBV_WC_RECV:
        conn = wc.wr_id
        print(conn)
        print("received message:", conn.recv_region)
    elif wc.opcode == e.IBV_WC_SEND:
        print("send completed successfully")


class RdmaServer(Node):
    # sockaddr_in addr
    # rdma_cm_event event
    # rdma_cm_id listener
    # rdma_event_channel ec
    # port
    def __init__(self, addr, port, name, options=c.OPTIONS):
        super().__init__(addr, port, name, is_server=True, options=options)
        print("ready to listen on ", addr + ":" + port)

        # event loop map config
        self.event_map = {
            ce.RDMA_CM_EVENT_CONNECT_REQUEST: self._on_connect_request,
            ce.RDMA_CM_EVENT_ESTABLISHED: self._on_established,
            ce.RDMA_CM_EVENT_REJECTED: self._on_rejected,
        }
        self.event_id = None

    def run(self):
        # bind_addr: will bind context and pd
        self.cid.bind_addr(self.addr_info)
        self.cid.listen(backlog=10)
        print("listening... ")
        while True:
            # block until the event come
            self.event = CMEvent(self.event_channel)
            print(self.event.event_type, self.event.event_str())
            if self.event_id is None:
                # next all action is done by this event_id, not cid
                self.event_id = CMID(creator=self.event, listen_id=self.cid)
            self.event_map[self.event.event_type]()
            self.event.ack_cm_event()

    def _on_connect_request(self):
        print("received connection request")
        self.prepare_resource(self.event_id)
        conn_param = ConnParam(resources=3, depth=3)
        # accept not use the origin server cmid, have to use the cmid in the events
        self.event_id.accept(conn_param)
        print("server accept")

    def _on_established(self):
        # need to poll cq and ack
        self.process_work_completion_events()

    def _on_rejected(self):
        self.close()
        print("rejected?!")

    def close(self):
        self.cid.close()
        self.addr_info.close()

