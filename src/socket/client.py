import socket
# config
import src.config.config as c
# common
from src.common.common import print_info, DONE_MSG
from src.common.socket_node import SocketNode
from src.common.buffer_attr import serialize, deserialize


class SocketClient:
    def __init__(self, name=c.NAME, addr=c.ADDR, port=c.PORT_INT, options=c.OPTIONS):
        self.name = name
        self.addr = addr
        self.port = port
        self.options = options
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def request(self):
        print("connect in", self.addr, self.port)
        self.socket.connect((self.addr, self.port))
        # use socket to exchange metadata of client
        node = SocketNode(name=self.name)
        node.prepare_resource()
        buffer_attr_bytes = serialize(node.buffer_attr)
        self.socket.sendall(buffer_attr_bytes)
        # get the metadata from server
        server_metadata_attr_bytes = self.socket.recv(c.BUFFER_SIZE)
        server_metadata_attr = deserialize(server_metadata_attr_bytes)
        print_info("server metadata attr:\n" + str(server_metadata_attr))
        # qp
        node.qp2init().qp2rtr(server_metadata_attr).qp2rts()
        # exchange done, write message to buffer
        message = "a message from client"
        me_len = len(message)
        node.post_write(message, me_len, server_metadata_attr.remote_stag, server_metadata_attr.addr)
        node.process_work_completion_events()
        # read the message
        node.post_read(me_len, server_metadata_attr.remote_stag, server_metadata_attr.addr)
        print("read")
        node.process_work_completion_events()
        read_message = node.read_mr.read(me_len, 0)
        print_info("read from sever\n" + str(read_message))
        # done
        self.socket.sendall(DONE_MSG)
        self.socket.close()  # 关闭连接
