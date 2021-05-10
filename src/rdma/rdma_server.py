# rdma server
# config
import copy

from pyverbs.mr import MR
from pyverbs.wr import SGE, SendWR

import src.config.config as c
import pyverbs.cm_enums as ce
import pyverbs.enums as e
# common
from src.common.common import die, print_info
from src.common.rdma_node import Node
from src.common.buffer_attr import BufferAttr, deserialize, serialize
# pyverbs
from pyverbs.cmid import CMID, CMEvent, ConnParam


class RdmaServer(Node):
    def __init__(self, addr, port, name, options=c.OPTIONS):
        super().__init__(addr, port, name, is_server=True, options=options)
        print("ready to listen on ", addr + ":" + port)

        # event loop map config
        self.event_map = {
            ce.RDMA_CM_EVENT_CONNECT_REQUEST: self._on_connect_request,
            ce.RDMA_CM_EVENT_ESTABLISHED: self._on_established,
            ce.RDMA_CM_EVENT_DISCONNECTED: self._on_disconnected,
            ce.RDMA_CM_EVENT_REJECTED: self._on_rejected,
        }
        self.event_id = None

    def run(self):
        # bind_addr: will bind context and pd
        self.cid.bind_addr(self.addr_info)
        self.cid.listen(backlog=10)
        # self.cid.get_request()
        print("listening... ")
        while True:
            # block until the event come
            event = CMEvent(self.event_channel)
            print(event.event_type, event.event_str())
            # next all action is done by this event_id, not cid
            event_type = event.event_type
            if self.event_id is None and event_type == ce.RDMA_CM_EVENT_CONNECT_REQUEST:
                # create a thread to deal with the request
                self.event_id = CMID(creator=event, listen_id=self.cid)
            self.event_map[event_type]()
            # create a new thread to deal the request
            event.ack_cm_event()

    def _on_connect_request(self):
        self.prepare_resource(self.event_id)
        conn_param = ConnParam(resources=3, depth=3)
        # accept not use the origin server cmid, have to use the cmid in the events
        self.event_id.accept(conn_param)

    def _on_established(self):
        # need to poll cq and ack
        self.process_work_completion_events()
        # get the client metadata attr
        client_metadata_attr = deserialize(self.metadata_recv_mr.read(c.BUFFER_SIZE, 0))
        print_info("client metadata attr:\n"+str(client_metadata_attr))
        self.init_mr(client_metadata_attr.length)
        buffer_attr_bytes = serialize(self.buffer_attr)
        self.metadata_send_mr.write(buffer_attr_bytes, len(buffer_attr_bytes))
        # sge = SGE(addr=self.metadata_send_mr.buf, length=c.BUFFER_SIZE, lkey=self.metadata_send_mr.lkey)
        # wr = SendWR(num_sge=1, sg=[sge])
        # self.qp.post_send(wr)
        self.event_id.post_send(self.metadata_send_mr)
        self.process_work_completion_events()

    def _on_disconnected(self):
        self.event_id.close()
        self.event_id = None

    def _on_rejected(self):
        self.close()
        print("rejected?!")

    def close(self):
        self.cid.close()
        self.addr_info.close()
