import json
import os
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from Assignment2.db_manager import DatabaseManager
from Assignment2.table import Table


USERS_SCHEMA = {
    "user_id": int,
    "name": str,
    "balance": float,
    "city": str,
}

PRODUCTS_SCHEMA = {
    "product_id": int,
    "name": str,
    "stock": int,
    "price": float,
}

ORDERS_SCHEMA = {
    "order_id": int,
    "user_id": int,
    "product_id": int,
    "amount": float,
    "time": str,
}


@dataclass
class TransactionState:
    tx_id: int
    active: bool = True
    held_locks: List[str] = field(default_factory=list)
    undo_log: List[Dict[str, Any]] = field(default_factory=list)


class ACIDDatabase:
    """Assignment 3 database engine using Assignment 2 B+Tree-backed tables."""

    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self._database_name = "assignment3"
        os.makedirs(self.data_dir, exist_ok=True)
        self.wal_path = os.path.join(self.data_dir, "wal.jsonl")
        self.snapshot_paths = {
            "users": os.path.join(self.data_dir, "assignment3_users_bplustree.json"),
            "products": os.path.join(self.data_dir, "assignment3_products_bplustree.json"),
            "orders": os.path.join(self.data_dir, "assignment3_orders_bplustree.json"),
        }

        self._tables = self._build_tables()
        self._table_locks = {name: threading.Lock() for name in self._tables}
        self._tx_lock = threading.Lock()
        self._next_tx_id = 1
        self._transactions: Dict[int, TransactionState] = {}

        if not os.path.exists(self.wal_path):
            with open(self.wal_path, "w", encoding="utf-8"):
                pass

        self._load_table_snapshots()
        self.last_recovery = self._recover_from_wal()
        self._persist_table_snapshots()

    def _build_tables(self) -> Dict[str, Table]:
        self._db_manager = DatabaseManager()
        self._db_manager.create_database(self._database_name)

        table_specs = [
            ("users", USERS_SCHEMA, "user_id"),
            ("products", PRODUCTS_SCHEMA, "product_id"),
            ("orders", ORDERS_SCHEMA, "order_id"),
        ]

        for table_name, schema, search_key in table_specs:
            self._db_manager.create_table(
                db_name=self._database_name,
                table_name=table_name,
                schema=schema,
                order=8,
                search_key=search_key,
            )

        return {
            table_name: self._db_manager.get_table(self._database_name, table_name)
            for table_name, _, _ in table_specs
        }

    def _append_wal(self, entry: Dict[str, Any]) -> None:
        entry = dict(entry)
        entry["ts"] = time.time()
        with open(self.wal_path, "a", encoding="utf-8") as wal_file:
            wal_file.write(json.dumps(entry, separators=(",", ":")) + "\n")
            wal_file.flush()
            os.fsync(wal_file.fileno())

    def _load_table_snapshots(self) -> None:
        for table_name, file_path in self.snapshot_paths.items():
            if not os.path.exists(file_path):
                continue

            with open(file_path, "r", encoding="utf-8") as table_file:
                snapshot = json.load(table_file)

            self._tables[table_name].import_snapshot(snapshot)

    def _persist_table_snapshots(self) -> None:
        for table_name, file_path in self.snapshot_paths.items():
            tmp_path = f"{file_path}.tmp"
            table_snapshot = self._tables[table_name].export_snapshot()

            with open(tmp_path, "w", encoding="utf-8") as table_file:
                json.dump(table_snapshot, table_file, indent=2)
                table_file.flush()
                os.fsync(table_file.fileno())

            os.replace(tmp_path, file_path)

    def _recover_from_wal(self) -> Dict[str, Any]:
        entries: List[Dict[str, Any]] = []
        with open(self.wal_path, "r", encoding="utf-8") as wal_file:
            for line in wal_file:
                line = line.strip()
                if not line:
                    continue
                entries.append(json.loads(line))

        committed = set()
        rolled_back = set()
        started = set()
        op_records: List[Dict[str, Any]] = []

        for entry in entries:
            e_type = entry.get("type")
            tx_id = entry.get("tx_id")
            if e_type == "BEGIN":
                started.add(tx_id)
            elif e_type == "OP":
                op_records.append(entry)
            elif e_type == "COMMIT":
                committed.add(tx_id)
            elif e_type == "ROLLBACK":
                rolled_back.add(tx_id)

        for record in op_records:
            if record["tx_id"] in committed:
                self._apply_operation(record, for_recovery=True)

        incomplete = sorted(started - committed - rolled_back)
        return {
            "committed_tx_count": len(committed),
            "rolled_back_tx_count": len(rolled_back),
            "incomplete_tx_ids_ignored": incomplete,
        }

    def _apply_operation(self, op_record: Dict[str, Any], for_recovery: bool = False) -> None:
        table = self._tables[op_record["table"]]
        op = op_record["op"]
        key = op_record["key"]
        before = op_record.get("before")
        after = op_record.get("after")

        if op == "insert":
            if table.get(key) is None:
                table.insert(after)
            return

        if op == "update":
            if table.get(key) is None:
                if for_recovery and before is not None:
                    # Recovery can safely recreate then update in rare partial-log scenarios.
                    table.insert(before)
                else:
                    raise KeyError(f"record not found for update: {op_record}")
            table.update(key, after)
            return

        if op == "delete":
            table.delete(key)
            return

        raise ValueError(f"unknown operation '{op}'")

    def begin(self) -> int:
        with self._tx_lock:
            tx_id = self._next_tx_id
            self._next_tx_id += 1
            self._transactions[tx_id] = TransactionState(tx_id=tx_id)

        self._append_wal({"type": "BEGIN", "tx_id": tx_id})
        return tx_id

    def _get_tx(self, tx_id: int) -> TransactionState:
        tx = self._transactions.get(tx_id)
        if tx is None or not tx.active:
            raise ValueError(f"transaction {tx_id} is not active")
        return tx

    def _acquire_table_lock(self, tx: TransactionState, table_name: str) -> None:
        if table_name in tx.held_locks:
            return
        self._table_locks[table_name].acquire()
        tx.held_locks.append(table_name)

    def _release_table_locks(self, tx: TransactionState) -> None:
        for table_name in reversed(tx.held_locks):
            self._table_locks[table_name].release()
        tx.held_locks.clear()

    def _validate_non_negative(self, table_name: str, record: Dict[str, Any]) -> None:
        if table_name == "users" and record["balance"] < 0:
            raise ValueError("users.balance must be non-negative")
        if table_name == "products":
            if record["stock"] < 0:
                raise ValueError("products.stock must be non-negative")
            if record["price"] < 0:
                raise ValueError("products.price must be non-negative")
        if table_name == "orders" and record["amount"] < 0:
            raise ValueError("orders.amount must be non-negative")

    def _validate_foreign_keys(self, table_name: str, record: Dict[str, Any]) -> None:
        if table_name != "orders":
            return

        user_exists = self._tables["users"].get(record["user_id"]) is not None
        if not user_exists:
            raise ValueError(f"orders.user_id {record['user_id']} does not reference users")

        product_exists = self._tables["products"].get(record["product_id"]) is not None
        if not product_exists:
            raise ValueError(f"orders.product_id {record['product_id']} does not reference products")

    def _validate_delete_constraints(self, table_name: str, key: Any) -> None:
        if table_name == "users":
            for order in self._tables["orders"].get_all():
                if order["user_id"] == key:
                    raise ValueError("cannot delete user referenced by orders")

        if table_name == "products":
            for order in self._tables["orders"].get_all():
                if order["product_id"] == key:
                    raise ValueError("cannot delete product referenced by orders")

    def insert(self, tx_id: int, table_name: str, record: Dict[str, Any]) -> Any:
        tx = self._get_tx(tx_id)
        self._acquire_table_lock(tx, table_name)

        table = self._tables[table_name]
        table.validate_record(record)
        self._validate_non_negative(table_name, record)
        self._validate_foreign_keys(table_name, record)

        key = record[table.search_key]
        before = None
        after = record.copy()

        self._append_wal(
            {
                "type": "OP",
                "tx_id": tx_id,
                "table": table_name,
                "op": "insert",
                "key": key,
                "before": before,
                "after": after,
            }
        )
        table.insert(after)
        tx.undo_log.append(
            {
                "table": table_name,
                "op": "insert",
                "key": key,
                "before": before,
                "after": after,
            }
        )
        return key

    def update(self, tx_id: int, table_name: str, key: Any, patch: Dict[str, Any]) -> bool:
        tx = self._get_tx(tx_id)
        self._acquire_table_lock(tx, table_name)

        table = self._tables[table_name]
        current = table.get(key)
        if current is None:
            raise KeyError(f"record with key {key} not found in {table_name}")

        if table.search_key in patch and patch[table.search_key] != key:
            raise ValueError("primary key updates are not allowed in Assignment 3")

        updated = current.copy()
        updated.update(patch)
        table.validate_record(updated)
        self._validate_non_negative(table_name, updated)
        self._validate_foreign_keys(table_name, updated)

        self._append_wal(
            {
                "type": "OP",
                "tx_id": tx_id,
                "table": table_name,
                "op": "update",
                "key": key,
                "before": current,
                "after": updated,
            }
        )
        table.update(key, updated)
        tx.undo_log.append(
            {
                "table": table_name,
                "op": "update",
                "key": key,
                "before": current,
                "after": updated,
            }
        )
        return True

    def delete(self, tx_id: int, table_name: str, key: Any) -> bool:
        tx = self._get_tx(tx_id)
        self._acquire_table_lock(tx, table_name)

        table = self._tables[table_name]
        current = table.get(key)
        if current is None:
            return False

        self._validate_delete_constraints(table_name, key)

        self._append_wal(
            {
                "type": "OP",
                "tx_id": tx_id,
                "table": table_name,
                "op": "delete",
                "key": key,
                "before": current,
                "after": None,
            }
        )
        table.delete(key)
        tx.undo_log.append(
            {
                "table": table_name,
                "op": "delete",
                "key": key,
                "before": current,
                "after": None,
            }
        )
        return True

    def read(self, tx_id: int, table_name: str, key: Any) -> Optional[Dict[str, Any]]:
        tx = self._get_tx(tx_id)
        self._acquire_table_lock(tx, table_name)
        record = self._tables[table_name].get(key)
        return None if record is None else record.copy()

    def commit(self, tx_id: int) -> None:
        tx = self._get_tx(tx_id)
        if not self.validate_all_constraints():
            self.rollback(tx_id)
            raise ValueError("consistency check failed during commit; transaction rolled back")

        self._append_wal({"type": "COMMIT", "tx_id": tx_id})
        self._persist_table_snapshots()
        tx.active = False
        self._release_table_locks(tx)
        with self._tx_lock:
            self._transactions.pop(tx_id, None)

    def rollback(self, tx_id: int) -> None:
        tx = self._get_tx(tx_id)

        for record in reversed(tx.undo_log):
            table = self._tables[record["table"]]
            op = record["op"]
            key = record["key"]
            before = record["before"]

            if op == "insert":
                table.delete(key)
            elif op == "update":
                table.update(key, before)
            elif op == "delete":
                table.insert(before)

        self._append_wal({"type": "ROLLBACK", "tx_id": tx_id})
        tx.active = False
        self._release_table_locks(tx)
        with self._tx_lock:
            self._transactions.pop(tx_id, None)

    def validate_all_constraints(self) -> bool:
        users = {record["user_id"] for record in self._tables["users"].get_all()}
        products = {record["product_id"] for record in self._tables["products"].get_all()}

        for user in self._tables["users"].get_all():
            self._validate_non_negative("users", user)

        for product in self._tables["products"].get_all():
            self._validate_non_negative("products", product)

        for order in self._tables["orders"].get_all():
            self._validate_non_negative("orders", order)
            if order["user_id"] not in users:
                return False
            if order["product_id"] not in products:
                return False

        return True

    def get_record(self, table_name: str, key: Any) -> Optional[Dict[str, Any]]:
        record = self._tables[table_name].get(key)
        return None if record is None else record.copy()

    def get_all_records(self, table_name: str) -> List[Dict[str, Any]]:
        return [record.copy() for record in self._tables[table_name].get_all()]

    def get_database_name(self) -> str:
        return self._database_name

    def list_tables(self) -> List[str]:
        return self._db_manager.list_tables(self._database_name)

    def range_query(self, table_name: str, start_key: Any, end_key: Any) -> List[Dict[str, Any]]:
        return [record.copy() for record in self._tables[table_name].range_query(start_key, end_key)]