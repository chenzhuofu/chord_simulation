from ..chord.chord_base import BaseChordNode
from ..chord.chord_base import connect_node, hash_func, is_between
from ..chord.struct_class import KeyValueResult, Node, KVStatus


class ChordNode(BaseChordNode):
    def __init__(self, address, port):
        super().__init__()

        self.node_id = hash_func(f'{address}:{port}')
        self.kv_store = dict()

        self.self_node = Node(self.node_id, address, port)
        self.successor = self.self_node
        self.predecessor = Node(self.node_id, address, port, valid=False)

        self.logger.info(f'node {self.node_id} listening at {address}:{port}')

    def _log_self(self):
        msg = 'now content: '
        for k, v in self.kv_store.items():
            msg += f'hash_func({k})={hash_func(k)}: {v}; '
        self.logger.debug(msg)

        pre_node_id = self.predecessor.node_id if self.predecessor.valid else "null"
        self.logger.debug(f"{pre_node_id} - {self.node_id} - {self.successor.node_id}")

    def lookup(self, key: str) -> KeyValueResult:
        h = hash_func(key)
        tmp_key_node = Node(h, "", 0)
        if is_between(tmp_key_node, self.predecessor, self.self_node):
            return self._lookup_local(key)
        else:
            next_node = self._closet_preceding_node(h)
            conn_next_node = connect_node(next_node)
            return conn_next_node.lookup(key)

    def _lookup_local(self, key: str) -> KeyValueResult:
        result = self.kv_store.get(key, None)
        status = KVStatus.VALID if result is not None else KVStatus.NOT_FOUND
        return KeyValueResult(key, result, self.node_id, status)

    def find_successor(self, key_id: int) -> Node:
        key_id_node = Node(key_id, "", 0)
        if is_between(key_id_node, self.self_node, self.successor):
            return self.self_node
        else:
            next_node = self._closet_preceding_node(key_id)
            conn_next_node = connect_node(next_node)
            return conn_next_node.find_successor(key_id)

    def _closet_preceding_node(self, key_id: int) -> Node:
        return self.successor

    def put(self, key: str, value: str) -> KeyValueResult:
        h = hash_func(key)
        tmp_key_node = Node(h, "", 0)
        if is_between(tmp_key_node, self.predecessor, self.self_node):
            return self.__do_put(key, value)
        else:
            next_node = self._closet_preceding_node(h)
            conn_next_node = connect_node(next_node)
            return conn_next_node.put(key, value)

    def __do_put(self, key: str, value: str) -> KeyValueResult:
        self.kv_store[key] = value
        return KeyValueResult(key, value, self.node_id)

    def join(self, node: Node):
        conn_node = connect_node(node)
        self.successor = conn_node.find_successor(self.node_id)

    def notify(self, node: Node):
        if not self.predecessor.valid or is_between(node, self.predecessor, self.self_node):
            self.predecessor = node

    def _stabilize(self):
        conn_successor = connect_node(self.successor)
        x = conn_successor.get_predecessor()
        if is_between(x, self.self_node, self.successor):
            self.successor = x

        conn_successor = connect_node(self.successor)
        conn_successor.notify(self.self_node)

    def _fix_fingers(self):
        pass

    def _check_predecessor(self):
        pass
