import socket
# config
import src.config.config as c
# common
from src.common.common import print_info
import src.common.msg as m
from src.common.buffer_attr import deserialize, serialize
from src.socket.socket_node import SocketNode


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
            node = SocketNode(self.name)
            node.prepare_resource()
            print("\n---------------------------- A CONNECT ACCEPT  --------------------------------")
            # event loop
            while True:
                try:
                    msg = conn.recv(c.BUFFER_SIZE)
                    if msg == m.BEGIN_MSG:
                        print("begin, exchange the metadata")
                        conn.sendall(m.READY_MSG)
                        # exchange the metadata
                        # use socket to exchange the metadata of server
                        client_metadata_attr_bytes = conn.recv(c.BUFFER_SIZE)
                        client_metadata_attr = deserialize(client_metadata_attr_bytes)
                        print_info("the client metadata attr is:\n" + str(client_metadata_attr))
                        # qp_attr
                        node.qp2init().qp2rtr(client_metadata_attr).qp2rts()
                        node.post_recv(node.recv_mr)
                        # send its buffer attr to client
                        buffer_attr_bytes = serialize(node.buffer_attr)
                        conn.sendall(buffer_attr_bytes)
                        # exchange metadata done
                        # node.process_work_completion_events()
                    elif msg == m.PUSH_FILE_MSG:
                        node.save_file()
                        print("success save file")
                    elif msg == m.DONE_MSG:
                        print("done")
                        node.close()
                        break

                except Exception as err:
                    print("error", err)
                    break
            print("---------------------------- A CONNECT DONE  --------------------------------")
            conn.close()
