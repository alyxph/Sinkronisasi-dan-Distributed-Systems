from src.nodes.lock_manager import LockStateMachine


def test_lock_fairness_shared_then_exclusive():
    sm = LockStateMachine()
    res1 = sm.apply(
        {
            "op": "acquire",
            "request_id": "r1",
            "lock": "alpha",
            "client_id": "c1",
            "mode": "shared",
            "created_at": 1.0,
        }
    )
    assert res1["status"] == "granted"

    res2 = sm.apply(
        {
            "op": "acquire",
            "request_id": "r2",
            "lock": "alpha",
            "client_id": "c2",
            "mode": "exclusive",
            "created_at": 2.0,
        }
    )
    assert res2["status"] == "waiting"

    res3 = sm.apply(
        {
            "op": "acquire",
            "request_id": "r3",
            "lock": "alpha",
            "client_id": "c3",
            "mode": "shared",
            "created_at": 3.0,
        }
    )
    assert res3["status"] == "waiting"

    sm.apply({"op": "release", "lock": "alpha", "client_id": "c1"})
    status = sm.get_status("r2")
    assert status["status"] == "granted"
