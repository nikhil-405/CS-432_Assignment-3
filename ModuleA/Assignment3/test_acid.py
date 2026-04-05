import json
import os
import shutil
import sys
import threading
import time

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

try:
    from Assignment3.acid_db import ACIDDatabase
except ModuleNotFoundError:
    from acid_db import ACIDDatabase


TEST_DATA_DIR = os.path.join(os.path.dirname(__file__), "_acid_test_data")


def clean_test_dir() -> None:
    if os.path.exists(TEST_DATA_DIR):
        shutil.rmtree(TEST_DATA_DIR)
    os.makedirs(TEST_DATA_DIR, exist_ok=True)


def seed_base_data(db: ACIDDatabase) -> None:
    """Insert baseline rows in all parent tables used by most tests."""
    tx = db.begin()

    db.insert(
        tx,
        "users",
        {"user_id": 1, "name": "Aarav", "balance": 1000.0, "city": "Delhi"},
    )
    db.insert(
        tx,
        "users",
        {"user_id": 2, "name": "Isha", "balance": 500.0, "city": "Pune"},
    )

    db.insert(
        tx,
        "products",
        {"product_id": 101, "name": "Keyboard", "stock": 10, "price": 100.0},
    )
    db.insert(
        tx,
        "products",
        {"product_id": 102, "name": "Mouse", "stock": 20, "price": 50.0},
    )

    db.commit(tx)


def test_relations_are_inside_single_database() -> None:
    """Checks that all required relations are registered under one logical database."""
    clean_test_dir()
    db = ACIDDatabase(TEST_DATA_DIR)
    seed_base_data(db)

    assert db.get_database_name() == "assignment3"
    assert db.list_tables() == ["orders", "products", "users"]


def test_atomicity_crash_before_commit_rolls_back_all_tables() -> None:
    """Simulates a crash before commit and verifies no partial cross-table effects survive."""
    clean_test_dir()
    db = ACIDDatabase(TEST_DATA_DIR)
    seed_base_data(db)

    tx = db.begin()
    db.update(tx, "users", 1, {"balance": 900.0})
    db.update(tx, "products", 101, {"stock": 9})
    db.insert(
        tx,
        "orders",
        {
            "order_id": 5001,
            "user_id": 1,
            "product_id": 101,
            "amount": 100.0,
            "time": "2026-04-05T12:00:00",
        },
    )

    # Crash simulation: restart engine without committing this transaction.
    recovered = ACIDDatabase(TEST_DATA_DIR)

    assert recovered.get_record("users", 1)["balance"] == 1000.0
    assert recovered.get_record("products", 101)["stock"] == 10
    assert recovered.get_record("orders", 5001) is None


def test_atomicity_explicit_rollback_undoes_multi_relation_changes() -> None:
    """Verifies explicit rollback undoes all writes performed in the transaction."""
    clean_test_dir()
    db = ACIDDatabase(TEST_DATA_DIR)
    seed_base_data(db)

    tx = db.begin()
    db.update(tx, "users", 2, {"balance": 450.0})
    db.update(tx, "products", 102, {"stock": 18})
    db.insert(
        tx,
        "orders",
        {
            "order_id": 5002,
            "user_id": 2,
            "product_id": 102,
            "amount": 100.0,
            "time": "2026-04-05T12:10:00",
        },
    )
    db.rollback(tx)

    assert db.get_record("users", 2)["balance"] == 500.0
    assert db.get_record("products", 102)["stock"] == 20
    assert db.get_record("orders", 5002) is None


def test_consistency_rejects_negative_values() -> None:
    """Ensures non-negative constraints are enforced for balance, stock, and amount."""
    clean_test_dir()
    db = ACIDDatabase(TEST_DATA_DIR)
    seed_base_data(db)

    tx1 = db.begin()
    try:
        db.update(tx1, "users", 1, {"balance": -1.0})
        raise AssertionError("Expected failure for negative user balance")
    except ValueError:
        db.rollback(tx1)

    tx2 = db.begin()
    try:
        db.update(tx2, "products", 101, {"stock": -1})
        raise AssertionError("Expected failure for negative product stock")
    except ValueError:
        db.rollback(tx2)

    tx3 = db.begin()
    try:
        db.insert(
            tx3,
            "orders",
            {
                "order_id": 5003,
                "user_id": 1,
                "product_id": 101,
                "amount": -100.0,
                "time": "2026-04-05T12:20:00",
            },
        )
        raise AssertionError("Expected failure for negative order amount")
    except ValueError:
        db.rollback(tx3)

    assert db.get_record("users", 1)["balance"] == 1000.0
    assert db.get_record("products", 101)["stock"] == 10
    assert db.get_record("orders", 5003) is None


def test_consistency_rejects_invalid_foreign_keys() -> None:
    """Ensures orders cannot reference missing users or products."""
    clean_test_dir()
    db = ACIDDatabase(TEST_DATA_DIR)
    seed_base_data(db)

    tx1 = db.begin()
    try:
        db.insert(
            tx1,
            "orders",
            {
                "order_id": 5004,
                "user_id": 999,
                "product_id": 101,
                "amount": 100.0,
                "time": "2026-04-05T12:30:00",
            },
        )
        raise AssertionError("Expected foreign key failure for missing user")
    except ValueError:
        db.rollback(tx1)

    tx2 = db.begin()
    try:
        db.insert(
            tx2,
            "orders",
            {
                "order_id": 5005,
                "user_id": 1,
                "product_id": 999,
                "amount": 100.0,
                "time": "2026-04-05T12:31:00",
            },
        )
        raise AssertionError("Expected foreign key failure for missing product")
    except ValueError:
        db.rollback(tx2)


def test_consistency_prevents_delete_of_referenced_rows() -> None:
    """Checks referential consistency by blocking deletes on referenced parent rows."""
    clean_test_dir()
    db = ACIDDatabase(TEST_DATA_DIR)
    seed_base_data(db)

    tx_create = db.begin()
    db.insert(
        tx_create,
        "orders",
        {
            "order_id": 5006,
            "user_id": 1,
            "product_id": 101,
            "amount": 100.0,
            "time": "2026-04-05T12:35:00",
        },
    )
    db.commit(tx_create)

    tx_delete_user = db.begin()
    try:
        db.delete(tx_delete_user, "users", 1)
        raise AssertionError("Expected delete to fail for user referenced by orders")
    except ValueError:
        db.rollback(tx_delete_user)

    tx_delete_product = db.begin()
    try:
        db.delete(tx_delete_product, "products", 101)
        raise AssertionError("Expected delete to fail for product referenced by orders")
    except ValueError:
        db.rollback(tx_delete_product)

    assert db.get_record("users", 1) is not None
    assert db.get_record("products", 101) is not None


def test_isolation_reader_waits_for_writer_commit() -> None:
    """Shows serializable behavior: reader blocks while writer holds the table lock."""
    clean_test_dir()
    db = ACIDDatabase(TEST_DATA_DIR)
    seed_base_data(db)

    writer_has_lock = threading.Event()
    errors = []
    results = {}

    def txn_writer() -> None:
        try:
            tx1 = db.begin()
            db.update(tx1, "users", 1, {"balance": 800.0})
            writer_has_lock.set()
            time.sleep(0.6)
            db.commit(tx1)
        except Exception as exc:
            errors.append(exc)

    def txn_reader() -> None:
        try:
            writer_has_lock.wait()
            tx2 = db.begin()
            start = time.time()
            user = db.read(tx2, "users", 1)
            elapsed = time.time() - start
            results["elapsed"] = elapsed
            results["balance"] = user["balance"]
            db.commit(tx2)
        except Exception as exc:
            errors.append(exc)

    t1 = threading.Thread(target=txn_writer)
    t2 = threading.Thread(target=txn_reader)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert not errors
    assert results["elapsed"] >= 0.5
    assert results["balance"] == 800.0


def test_isolation_serializes_competing_writers() -> None:
    """Checks no lost update when two writers target the same row concurrently."""
    clean_test_dir()
    db = ACIDDatabase(TEST_DATA_DIR)
    seed_base_data(db)

    first_has_lock = threading.Event()
    errors = []
    results = {}

    def writer_one() -> None:
        try:
            tx1 = db.begin()
            db.update(tx1, "users", 1, {"balance": 900.0})
            first_has_lock.set()
            time.sleep(0.4)
            db.commit(tx1)
        except Exception as exc:
            errors.append(exc)

    def writer_two() -> None:
        try:
            first_has_lock.wait()
            tx2 = db.begin()
            seen_balance = db.read(tx2, "users", 1)["balance"]
            db.update(tx2, "users", 1, {"balance": seen_balance - 50.0})
            db.commit(tx2)
            results["seen_balance"] = seen_balance
        except Exception as exc:
            errors.append(exc)

    t1 = threading.Thread(target=writer_one)
    t2 = threading.Thread(target=writer_two)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert not errors
    assert results["seen_balance"] == 900.0
    assert db.get_record("users", 1)["balance"] == 850.0


def test_durability_committed_data_persists_after_restart() -> None:
    """Validates durability by committing then recreating the engine."""
    clean_test_dir()
    db = ACIDDatabase(TEST_DATA_DIR)
    seed_base_data(db)

    tx = db.begin()
    db.update(tx, "users", 1, {"balance": 900.0})
    db.update(tx, "products", 101, {"stock": 9})
    db.insert(
        tx,
        "orders",
        {
            "order_id": 5007,
            "user_id": 1,
            "product_id": 101,
            "amount": 100.0,
            "time": "2026-04-05T13:00:00",
        },
    )
    db.commit(tx)

    restarted = ACIDDatabase(TEST_DATA_DIR)

    assert restarted.get_record("users", 1)["balance"] == 900.0
    assert restarted.get_record("products", 101)["stock"] == 9
    assert restarted.get_record("orders", 5007) is not None

    assert os.path.exists(os.path.join(TEST_DATA_DIR, "assignment3_users_bplustree.json"))
    assert os.path.exists(os.path.join(TEST_DATA_DIR, "assignment3_products_bplustree.json"))
    assert os.path.exists(os.path.join(TEST_DATA_DIR, "assignment3_orders_bplustree.json"))


def test_recovery_keeps_committed_and_discards_incomplete_transactions() -> None:
    """Checks restart recovery: committed tx survives, incomplete tx is ignored."""
    clean_test_dir()
    db = ACIDDatabase(TEST_DATA_DIR)
    seed_base_data(db)

    tx_committed = db.begin()
    db.update(tx_committed, "users", 1, {"balance": 950.0})
    db.commit(tx_committed)

    tx_incomplete = db.begin()
    db.update(tx_incomplete, "products", 101, {"stock": 5})
    db.insert(
        tx_incomplete,
        "orders",
        {
            "order_id": 5008,
            "user_id": 1,
            "product_id": 101,
            "amount": 500.0,
            "time": "2026-04-05T13:10:00",
        },
    )

    recovered = ACIDDatabase(TEST_DATA_DIR)

    assert recovered.get_record("users", 1)["balance"] == 950.0
    assert recovered.get_record("products", 101)["stock"] == 10
    assert recovered.get_record("orders", 5008) is None


def test_bplustree_json_snapshot_rebuilds_tree_for_queries_after_restart() -> None:
    """Verifies JSON snapshot keeps full tree structure and range-query behavior after restart."""
    clean_test_dir()
    db = ACIDDatabase(TEST_DATA_DIR)
    seed_base_data(db)

    # Insert enough rows to force B+ tree node splits.
    tx = db.begin()
    for user_id in range(3, 26):
        db.insert(
            tx,
            "users",
            {
                "user_id": user_id,
                "name": f"User{user_id}",
                "balance": float(100 + user_id),
                "city": f"City{user_id}",
            },
        )
    db.commit(tx)

    users_snapshot_path = os.path.join(TEST_DATA_DIR, "assignment3_users_bplustree.json")
    with open(users_snapshot_path, "r", encoding="utf-8") as snapshot_file:
        snapshot = json.load(snapshot_file)

    assert snapshot["search_key"] == "user_id"
    assert "tree" in snapshot
    assert "root" in snapshot["tree"]
    assert isinstance(snapshot["tree"]["root"], dict)

    restarted = ACIDDatabase(TEST_DATA_DIR)

    # If rebuild worked, indexed operations still behave correctly after restart.
    range_rows = restarted.range_query("users", 5, 8)
    assert [row["user_id"] for row in range_rows] == [5, 6, 7, 8]
    assert restarted.get_record("users", 22)["city"] == "City22"


def run_all_tests() -> None:
    tests = [
        ("Single database with three relations", test_relations_are_inside_single_database),
        ("Atomicity on crash before commit", test_atomicity_crash_before_commit_rolls_back_all_tables),
        ("Atomicity on explicit rollback", test_atomicity_explicit_rollback_undoes_multi_relation_changes),
        ("Consistency: non-negative constraints", test_consistency_rejects_negative_values),
        ("Consistency: foreign key constraints", test_consistency_rejects_invalid_foreign_keys),
        ("Consistency: protected parent deletes", test_consistency_prevents_delete_of_referenced_rows),
        ("Isolation: reader waits for writer", test_isolation_reader_waits_for_writer_commit),
        ("Isolation: competing writers serialized", test_isolation_serializes_competing_writers),
        ("Durability after restart", test_durability_committed_data_persists_after_restart),
        ("Recovery of committed vs incomplete", test_recovery_keeps_committed_and_discards_incomplete_transactions),
        (
            "B+ tree JSON rebuild and query behavior",
            test_bplustree_json_snapshot_rebuilds_tree_for_queries_after_restart,
        ),
    ]

    print("Running Assignment 3 ACID validation tests...")
    for name, test_fn in tests:
        test_fn()
        print(f"[PASS] {name}")

    print("All ACID tests passed.")


if __name__ == "__main__":
    run_all_tests()