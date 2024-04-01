namespace py chord

service ChordNode {
    KeyValueResult lookup(1: string key),
    Node find_successor(1: i32 key_id),
    KeyValueResult put(1: string key, 2: string value),
    void join(1: Node node),
    void notify(1: Node node),
    Node get_predecessor(),
    DataShard get_data_shard(1: i32 id),
}

enum KVStatus {
    VALID, NOT_FOUND
}

struct KeyValueResult {
    1: string key,
    2: string value,
    3: i32 node_id,
    4: KVStatus status,
}

struct Node {
    1: i32 node_id,
    2: string address,
    3: i32 port,
    4: bool valid,
}

struct Data {
    1: string key,
    2: string value,
    3: KVStatus status,
}

struct DataShard {
    1: list<Data> data,
}
