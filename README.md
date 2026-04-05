# CS-432 Database Systems — Assignment 3

A two-module project for **CS-432 Database Systems** that builds a custom database engine from scratch (Module A) and layers a full-stack web application on top of a MySQL backend (Module B).

| Module | Focus | Stack |
|--------|-------|-------|
| **Module A** | Custom B+ Tree engine, performance benchmarks, ACID transactions | Python, matplotlib, graphviz |
| **Module B** | Web app with RBAC, SQL indexing, stress testing | Python, Flask, MySQL, SQLAlchemy |

---



## Module A — Custom Database Engine


The **ACIDDatabase** engine (`acid_db.py`) wraps the B+ Tree tables with full transactional guarantees the ACID properties.

The **test suite** (`test_acid.py`) contains 11 automated tests verifying crash recovery, explicit rollback, constraint enforcement, concurrent isolation, durability after restart, and B+ Tree snapshot fidelity.

#### Running the ACID Tests

```bash
cd ModuleA
python -m Assignment3.test_acid
```

---

## Module B — Flask Web Application (SafeDocs)

### Assignment 2: RBAC, CRUD & SQL Optimization

A full-stack **Flask** web application with:

- **Session-based authentication** with Admin / Regular user roles
- **Document management** — CRUD APIs with password-protected document viewing
- **Member management** — Admin-only member creation and deletion
- **Permission system** — Grant/revoke per-document access (View, Edit, Delete)
- **Audit logging** — All security events logged to both a database table and a local file
- **SQL optimization** — Index strategy with EXPLAIN-based profiling and benchmarking

**UI pages**: Login, Dashboard, Members, Documents list, Document viewer (password-gated)

#### Setup

1. Copy `.env.example` to `.env` and set your MySQL credentials:
   ```
   DB_USER=root
   DB_PASSWORD=your_password
   DB_HOST=localhost
   DB_PORT=3306
   DB_NAME=safedocs
   JWT_SECRET=your_secret
   DEFAULT_ADMIN_USERNAME=admin
   DEFAULT_ADMIN_PASSWORD=admin_pass
   ```



2. Run the app:
   ```bash
   cd Module_B/Assignment\ 2
   python -m module_B.app
   ```


See [`Module_B/Assignment 2/module_B/README.md`](Module_B/Assignment%202/module_B/README.md) for the full API reference.

### Assignment 3: Multi-User Stress Testing

A Jupyter notebook (`module_b_stress_test.ipynb`) that subjects the system to:

- **High-concurrency load testing** — simultaneous user sessions
- **Race condition analysis** — competing writes to shared resources
- **Failure simulation** — verifying ACID properties under stress

Results are exported to `reports/` as both JSON data and a Markdown summary.

#### Setup

1. Run the app:
   ```bash
   cd Module_B/Assignment\ 2
   python -m module_B.app
   ```

2. Run `module_b_stress_test.ipynb`

---


## Repository Structure

```
CS-432_Assignment-3/
├── ModuleA/
│   ├── Assignment2/          # B+ Tree engine & performance benchmarks
│   │   ├── bplustree.py      # Full B+ Tree implementation (insert, delete, range query, serialization)
│   │   ├── table.py          # Schema-validated table abstraction over the B+ Tree
│   │   ├── db_manager.py     # In-memory database/table registry
│   │   ├── bruteforce.py     # Brute-force baseline for benchmarking
│   │   └── performance.py    # Comparative benchmarks (time & memory) with matplotlib plots
│   ├── Assignment3/          # ACID-compliant transactional layer
│   │   ├── acid_db.py        # ACIDDatabase engine (WAL, locking, recovery, constraints)
│   │   └── test_acid.py      # 11 validation tests covering all ACID properties
│   └── requirements.txt
│
├── Module_B/
│   ├── Assignment 2/         # Flask web app with RBAC & SQL optimization
│   │   └── module_B/
│   │       ├── app.py            # Application entry point
│   │       ├── __init__.py       # App factory & bootstrap
│   │       ├── routes.py         # UI & API routes
│   │       ├── auth.py           # Authentication & session decorators
│   │       ├── models.py         # SQLAlchemy models (Users, Documents, Permissions, etc.)
│   │       ├── database.py       # DB session & bootstrap helpers
│   │       ├── audit.py          # Audit logging (file + table)
│   │       ├── config.py         # Environment-based configuration
│   │       ├── benchmark.py      # Query timing & EXPLAIN profiling
│   │       ├── query_analysis.py # Query analysis utilities
│   │       ├── sql/
│   │       │   ├── create_core_tables.sql
│   │       │   └── indexes.sql
│   │       ├── templates/        # Jinja2 UI pages (login, dashboard, documents, etc.)
│   │       └── README.md         # Detailed Module B documentation
│   │
│   └── Assignment 3/         # Multi-user stress testing
│       └── module_b/
│           ├── module_b_stress_test.ipynb   # Concurrency & ACID stress test notebook
│           └── reports/                     # Generated stress test reports (JSON + Markdown)
│
├── LICENSE                   # MIT License
└── .gitignore
```

---