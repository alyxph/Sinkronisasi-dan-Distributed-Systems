from src.utils.hashing import ConsistentHashRing


def test_consistent_hash_stable_mapping():
    ring = ConsistentHashRing(["n1", "n2", "n3"], replicas=10)
    node_a = ring.get_node("queue-a")
    node_b = ring.get_node("queue-a")
    assert node_a == node_b


def test_consistent_hash_after_removal():
    ring = ConsistentHashRing(["n1", "n2", "n3"], replicas=10)
    ring.set_nodes(["n1", "n3"])
    node = ring.get_node("queue-a")
    assert node in {"n1", "n3"}
