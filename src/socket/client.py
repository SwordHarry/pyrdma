import socket
# config
import src.config.config as c
# common
from src.common.common import print_info
import src.common.msg as m
from src.socket.socket_node import SocketNode
from src.common.buffer_attr import serialize, deserialize

import asyncio


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
        self.socket.sendall(m.BEGIN_MSG)
        msg = self.socket.recv(c.BUFFER_SIZE)
        node = None
        if msg == m.READY_MSG:
            # use socket to exchange metadata of client
            node = SocketNode(name=self.name)
            buffer_attr_bytes = serialize(node.buffer_attr)
            self.socket.sendall(buffer_attr_bytes)
            # get the metadata from server
            server_metadata_attr_bytes = self.socket.recv(c.BUFFER_SIZE)
            server_metadata_attr = deserialize(server_metadata_attr_bytes)
            print_info("server metadata attr:\n" + str(server_metadata_attr))
            # qp
            node.qp2init().qp2rtr(server_metadata_attr).qp2rts()
            # exchange done, write message or push file to buffer
            # node.post_recv(node.recv_mr)
            self.socket.sendall(m.PUSH_FILE_MSG)
            node.push_file("./test/src/50M.file", server_metadata_attr.remote_stag, server_metadata_attr.addr)
            print("push done exist")
            # msg = "a message from client"
            # node.post_write(node.file_mr, msg, len(msg), server_metadata_attr.remote_stag, server_metadata_attr.addr)
            # node.poll_cq()
            # node.post_read(node.recv_mr, c.BUFFER_SIZE, server_metadata_attr.remote_stag, server_metadata_attr.addr)
            # node.poll_cq()
            # msg = node.recv_mr.read(c.BUFFER_SIZE, 0)
            # print(msg)
        # done
        self.socket.sendall(m.DONE_MSG)
        node.close()
        self.socket.close()  # 关闭连接
