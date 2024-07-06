from ..chord.chord_base import BaseChordNode
from ..chord.chord_base import connect_node, hash_func, is_between
from ..chord.struct_class import KeyValueResult, Node, KVStatus, M, R, Data, DataShard, Replica, Successors

# finger_table Chord implementation
class ChordNode(BaseChordNode):
    def __init__(self, address, port):
        super().__init__()
        self.address = address
        self.port = port
        self.node_id = hash_func(f'{address}:{port}')
        self.kv_store = dict()
        self.self_node = Node(self.node_id, address, port)
        
        self.predecessor = Node(self.node_id, address, port, valid=False)
        self.finger_table = [Node(self.node_id, address, port) for _ in range(M)]
        self.next = 0
        
        self.replicas = [Replica(-1, []) for _ in range(R - 1)]
        self.successors = [Node(self.node_id, address, port) for _ in range(R - 1)]
        
        self.logger.info(f'node {self.node_id} listening at {address}:{port}')

    def _log_self(self):
        msg = 'now content: '
        for k, v in self.kv_store.items():
            msg += f'hash_func({k})={hash_func(k)}: {v}; '
        self.logger.debug(msg)
        
        pre_node_id = self.predecessor.node_id if self.predecessor.valid else "null"
        self.logger.debug(f"{pre_node_id} - {self.node_id} - {self.finger_table[0].node_id}")
        self.logger.debug(f'finger_table: {self.finger_table}')

    def find_successor(self, key_id: int) -> Node:
        tmp_key_node = Node(key_id, "", 0)
        if is_between(tmp_key_node, self.self_node, self.finger_table[0]):
            return self.finger_table[0]
        else:
            try:
                next_node = self._closet_preceding_node(key_id)
                conn_next_node = connect_node(next_node)
                return conn_next_node.find_successor(key_id)
            except Exception as e:
                self.logger.info(f"find_successor failed, wait finger_table to repair: {e}")
                return self.finger_table[0]
    
    def _closet_preceding_node(self, key_id: int) -> Node:
        tmp_key_node = Node(( key_id - 1 + 2 ** M ) % (2 ** M), "", 0)
        for i in range(M-1, -1, -1):
            if is_between(self.finger_table[i], self.self_node, tmp_key_node):
                return self.finger_table[i]
        return self.self_node

    def lookup(self, key: str) -> KeyValueResult:
        key_id = hash_func(key)
        tmp_key_node = Node(key_id, "", 0)
        if is_between(tmp_key_node, self.predecessor, self.self_node):
            return self._lookup_local(key)
        else:
            node = self.find_successor(key_id)
            conn_node = connect_node(node)
            retry = 3
            while retry > 0:
                result = conn_node.lookup(key)
                if result.status == KVStatus.VALID:
                    return result
                retry -= 1
            return KeyValueResult(key, "", self.node_id, KVStatus.NOT_FOUND)
    
    def _lookup_local(self, key: str) -> KeyValueResult:
        result = self.kv_store.get(key, None)
        status = KVStatus.VALID if result is not None else KVStatus.NOT_FOUND
        return KeyValueResult(key, result, self.node_id, status)
    
    def put(self, key: str, value: str) -> KeyValueResult:
        key_id = hash_func(key)
        tmp_key_node = Node(key_id, "", 0)
        if is_between(tmp_key_node, self.predecessor, self.self_node):
            return self.__do_put(key, value)
        else:
            node = self.find_successor(key_id)
            conn_node = connect_node(node)
            return conn_node.put(key, value)
        
    def __do_put(self, key: str, value: str) -> KeyValueResult:
        self.kv_store[key] = value
        conn_successor = connect_node(self.finger_table[0])
        conn_successor.put_in_replica(1, key, value, self.node_id)
        return KeyValueResult(key, value, self.node_id)
    
    def join(self, node: Node):
        if node:
            conn_node = connect_node(node)
            self.finger_table[0] = conn_node.find_successor(self.node_id)
            self.predecessor = Node(self.node_id, self.address, self.port, valid=False)
            conn_successor = connect_node(self.finger_table[0])
            data_shard = conn_successor.get_data_shard(self.node_id)
            for data in data_shard.data:
                if data.status == KVStatus.NOT_FOUND:
                    break
                self.__do_put(data.key, data.value)
        else:
            for i in range(M):
                self.finger_table[i] = self.self_node
            self.predecessor = self.self_node
            
    def _stabilize(self):
        try:
            conn_successor = connect_node(self.finger_table[0])
            x = conn_successor.get_predecessor()
        except Exception as e:
            for i in range(R - 1):
                try:
                    conn_successor = connect_node(self.successors[i])
                    x = conn_successor.get_predecessor()
                    self.finger_table[0] = self.successors[i]
                    break
                except Exception as e:
                    continue
        if x is None:
            return
        if is_between(x, self.self_node, self.finger_table[0]):
            self.finger_table[0] = x
        try:
            conn_successor = connect_node(self.finger_table[0])
            conn_successor.notify(self.self_node)
        except Exception as e:
            self.logger.info(f"stabilize failed, wait finger_table to repair: {e}")
            return
        
    def notify(self, node: Node):
        if not self.predecessor.valid or is_between(node, self.predecessor, self.self_node):
            self.predecessor = node
            
    def _fix_fingers(self):
        self.next = (self.next + 1) % M
        key_id = (self.node_id + 2 ** self.next) % (2 ** M)
        self.finger_table[self.next] = self.find_successor(key_id)
    
    def _check_predecessor(self):
        if not self.predecessor.valid:
            return
        try:
            conn_predecessor = connect_node(self.predecessor)
            conn_predecessor.find_successor(self.node_id)
        except:
            self.predecessor = Node(self.node_id, self.address, self.port, valid=False)
            for data in self.replicas[0].data:
                self.kv_store[data.key] = data.value
            self.successors = [Node(self.node_id, self.address, self.port) for _ in range(R - 1)]
            
    def get_predecessor(self) -> Node:
        return self.predecessor
    
    def get_data_shard(self, id: int) -> DataShard:
        # all data satisfy: is_between(data, self.predecessor, id)
        # should remove locally
        tmp_id_node = Node(id, "", 0)
        data_shard = [ Data(k, v) for k, v in self.kv_store.items() if is_between(Node(hash_func(k), "", 0), self.predecessor, tmp_id_node) ]
        for data in data_shard:
            self.kv_store.pop(data.key)
        data_shard.append(Data("", "", KVStatus.NOT_FOUND))
        return DataShard(data_shard)
    
    def put_in_replica(self, step: int, key: str, value: str, node_id: int):
        if self.node_id == node_id:
            return
        self.replicas[step - 1].data.append(Data(key, value))
        step = step + 1
        if step < R:
            conn_node = connect_node(self.finger_table[0])
            conn_node.put_in_replica(step, key, value, node_id)
    
    def _check_replicas(self):
        try:
            conn_node = connect_node(self.finger_table[0])
            new_successors = conn_node.get_successors().successors
        except Exception as e:
            self.logger.info(f"check_replicas failed, wait finger_table to repair: {e}")
            return
        new_successors = [self.finger_table[0]] + new_successors[:R - 2]
        for i in range(R - 1):
            if new_successors[i] != self.successors[i]:
                try:
                    data = [ Data(k, v) for k, v in self.kv_store.items() ]
                    conn_node = connect_node(self.finger_table[0])
                    status = conn_node.update_replica(1, Replica(self.node_id, data))
                    if status == KVStatus.VALID:
                        self.successors = new_successors
                except Exception as e:
                    self.logger.info(f"check_replicas failed, wait finger_table to repair: {e}")
                    return
                break
    
    def update_replica(self, step: int, replica: Replica) -> KVStatus:
        if self.node_id == replica.node_id:
            return KVStatus.VALID
        self.replicas[step - 1] = replica
        step = step + 1
        if step < R:
            try:
                conn_node = connect_node(self.finger_table[0])
                conn_node.update_replica(step, replica)
            except Exception as e:
                self.logger.info(f"update_replica failed, wait finger_table to repair: {e}")
                return KVStatus.NOT_FOUND
        return KVStatus.VALID

    def get_successors(self) -> Successors:
        return Successors(self.successors)
