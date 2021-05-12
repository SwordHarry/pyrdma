# const
import pyverbs.cm_enums as ce
import pyverbs.enums as e
# config
import src.config.config as c
# common
from src.common.buffer_attr import BufferAttr, serialize, deserialize
from src.common.common import check_wc_status, print_info
# pyverbs: need v32
from pyverbs.cmid import CMID, AddrInfo
from pyverbs.mr import MR
from pyverbs.pd import PD
from pyverbs.qp import QPInitAttr, QPCap
# util
from src.rdma.socket.rdma_socket_util import process_wc_send_events, process_wc_recv_events


class RdmaSocketClient:
    def __init__(self, addr, port, options=c.OPTIONS):
        # addr_info = AddrInfo(dst=addr, dst_service=port, port_space=ce.RDMA_PS_TCP) # v32
        addr_info = AddrInfo(dst=addr, service=port, port_space=ce.RDMA_PS_TCP)
        qp_options = options["qp_init"]
        cap = QPCap(max_send_wr=qp_options["max_send_wr"], max_recv_wr=qp_options["max_recv_wr"],
                    max_send_sge=qp_options["max_send_sge"], max_recv_sge=qp_options["max_recv_sge"])
        qp_init_attr = QPInitAttr(qp_type=qp_options["qp_type"], cap=cap)
        self.sid = CMID(creator=addr_info, qp_init_attr=qp_init_attr)

    def request(self):
        # TODO: Failed to dealloc PD. Errno: 16, Device or resource busy??? i dont dealloc the pd?
        self.sid.connect()
        pd = PD(self.sid)
        resource_send_mr = MR(pd, c.BUFFER_SIZE,
                              e.IBV_ACCESS_LOCAL_WRITE | e.IBV_ACCESS_REMOTE_READ | e.IBV_ACCESS_REMOTE_WRITE)
        # metadata_send_mr: client send the resource_send_mr attr to server
        buffer_attr = BufferAttr(addr=resource_send_mr.buf, length=c.BUFFER_SIZE,
                                 local_stag=resource_send_mr.lkey, remote_stag=resource_send_mr.rkey)
        # metadata_send_mr: node send the resource_send_mr attr to others
        metadata_send_mr = MR(pd, c.BUFFER_SIZE, e.IBV_ACCESS_LOCAL_WRITE)
        buffer_attr_bytes = serialize(buffer_attr)
        metadata_send_mr.write(buffer_attr_bytes, len(buffer_attr_bytes))
        self.sid.post_send(metadata_send_mr)
        process_wc_send_events(self.sid)
        metadata_recv_mr = MR(pd, c.BUFFER_SIZE, e.IBV_ACCESS_LOCAL_WRITE)
        self.sid.post_recv(metadata_recv_mr)
        process_wc_recv_events(self.sid)
        server_metadata_attr_bytes = metadata_recv_mr.read(c.BUFFER_SIZE, 0)
        server_metadata_attr = deserialize(server_metadata_attr_bytes)
        print_info("server metadata attr:\n" + str(server_metadata_attr))
        # exchange done, write message to buffer
        message = "a message from client"
        me_len = len(message)
        resource_send_mr.write(message, me_len)
        self.sid.post_write(resource_send_mr, me_len,
                            server_metadata_attr.addr, server_metadata_attr.remote_stag)
        process_wc_send_events(self.sid)
        resource_read_mr = MR(pd, c.BUFFER_SIZE,
                              e.IBV_ACCESS_LOCAL_WRITE | e.IBV_ACCESS_REMOTE_READ | e.IBV_ACCESS_REMOTE_WRITE)
        self.sid.post_read(resource_read_mr, c.BUFFER_SIZE,
                           server_metadata_attr.addr, server_metadata_attr.remote_stag)
        process_wc_send_events(self.sid)
        read_me = resource_read_mr.read(me_len, 0)
        print_info("read from the server buffer:\n" + str(read_me))
        # write and read done, notify the server done
        message = "done"
        # me_len = len(message)
        resource_send_mr.write(message, c.BUFFER_SIZE)
        self.sid.post_send(resource_send_mr)
        process_wc_send_events(self.sid)
        self.sid.close()
