import socket
# config
import src.config.config as c
# common
from src.common.common import print_info, DONE_MSG
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
                    # qp_attr
                    node.qp2init().qp2rtr(client_metadata_attr)
                    # send its buffer attr to client
                    buffer_attr_bytes = serialize(node.buffer_attr)
                    conn.sendall(buffer_attr_bytes)
                    # exchange metadata done
                    done_msg = conn.recv(c.BUFFER_SIZE)
                    if done_msg == DONE_MSG:
                        break

                    print("---------------------------- A CONNECT DONE  --------------------------------")
                except Exception as err:
                    print("error", err)
                    break
            conn.close()
