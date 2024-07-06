from ..chord.chord_base import BaseChordNode
from ..chord.chord_base import connect_node, hash_func, is_between
from ..chord.struct_class import KeyValueResult, Node, KVStatus, M, Data, DataShard

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
            except Exception as e:
                if e == "Timeout":
                    next_node = self.find_successor(next_node.node_id - 1)
                    conn_next_node = connect_node(next_node)
            return conn_next_node.find_successor(key_id)
    
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
        conn_successor = connect_node(self.finger_table[0])
        x = conn_successor.get_predecessor()
        if is_between(x, self.self_node, self.finger_table[0]):
            self.finger_table[0] = x
        conn_successor = connect_node(self.finger_table[0])
        conn_successor.notify(self.self_node)
        
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
