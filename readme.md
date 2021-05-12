# pyrdma
a python3 rdma server and client using [rdma-core](https://github.com/linux-rdma/rdma-core) 
and [python3-pyverbs](https://github.com/linux-rdma/rdma-core/tree/master/pyverbs)

### environment
- Ubuntu_v20.04
- Roce
- rdma-core debian_v32.0.1 or debian_v28.0.1

### implement
- src.rdma.event: cmid and event loop(need v32)
- src.rdma.socket: cmid and socket(need v32)
- src.socket: socket and ibv

### server
```shell
python3 main_server.py
```
### client
```shell
python3 main_client.py
```
