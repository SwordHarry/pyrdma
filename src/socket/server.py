import socket

# const
import pyverbs.enums as e
from pyverbs.addr import AHAttr, GlobalRoute, GID
from src.common.common import print_info
from pyverbs.cmid import CMEventChannel, CMEvent
from pyverbs.qp import QPAttr
from pyverbs.wr import SGE, RecvWR, SendWR
import src.config.config as c
from src.common.buffer_attr import deserialize, serialize
from src.common.socket_node import SocketNode


# connection establish use socket, then use ibv to rdma
class SocketServer:
    def __init__(self, name=c.NAME, addr=c.ADDR, port=c.PORT_INT, options=c.OPTIONS):
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.bind((addr, port))
        self.server.listen(5)
        print("listening in", addr, port)
        self.name = name
        self.options = options

    def serve(self):
        while True:
            conn, addr = self.server.accept()
            while True:
                try:
                    print("\n---------------------------- A CONNECT ACCEPT  --------------------------------")
                    # use socket to exchange the metadata of server
                    client_metadata_attr_bytes = conn.recv(c.BUFFER_SIZE)
                    client_metadata_attr = deserialize(client_metadata_attr_bytes)
                    print_info("the client metadata attr is:\n" + str(client_metadata_attr))
                    node = SocketNode(self.name)
                    node.prepare_resource()
                    # qp
                    qp_attr = QPAttr(qp_state=e.IBV_QPS_RTS, cur_qp_state=node.qp.qp_state)
                    gid_options = self.options["gid_init"]
                    c_gid = GID(client_metadata_attr.gid)
                    gr = GlobalRoute(dgid=c_gid, sgid_index=gid_options["gid_index"])
                    ah_attr = AHAttr(gr=gr, is_global=1, port_num=gid_options["port_num"])
                    qp_attr.ah_attr = ah_attr
                    qp_attr.dest_qp_num = client_metadata_attr.qp_num
                    node.qp.to_rtr(qp_attr)
                    # send its buffer attr to client
                    buffer_attr_bytes = serialize(node.buffer_attr)
                    conn.sendall(buffer_attr_bytes)
                    # exchange metadata done
                    # sge = SGE(addr=node.resource_mr.buf, length=client_metadata_attr.length, lkey=node.resource_mr.lkey)
                    # wr = RecvWR(num_sge=1, sg=[sge, ])
                    # node.qp.post_recv(wr)
                    done_msg = conn.recv(c.BUFFER_SIZE)
                    if str(done_msg) == "done":
                        break
                    # read_message = node.resource_mr.read(c.BUFFER_SIZE, 0)
                    # print_info("read from resource_mr: "+str(read_message))
                    # self.qp.to_rtr(QPAttr(cur_qp_state=self.qp.qp_state))
                    print("---------------------------- A CONNECT DONE  --------------------------------")
                except Exception as err:
                    print("error", err)
                    break
            conn.close()
