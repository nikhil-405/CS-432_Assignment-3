# Module B — Stress Test & ACID Report

**Generated**: 2026-04-05 13:52:45

## Summary

- Overall: **ALL PASSED**
- Tests: 7/7 (100.0%)

## Results

| Scenario | Status | Reqs | OK | Fail | P50 ms | P95 ms | RPS |
|----------|--------|------|----|------|--------|--------|-----|
| Health Probe | PASS | 20 | 20 | 0 | 16.97 | 36.59 | 51.62 |
| Login Baseline | PASS | 20 | 20 | 0 | 138.81 | 157.46 | 7.29 |
| Access Control | PASS | 2 | 0 | 2 | 8.53 | 12.35 | 109.18 |
| Read Stress | PASS | 300 | 300 | 0 | 146.1 | 223.34 | 100.01 |
| Crud Lifecycle | PASS | 5 | 4 | 1 | 24.15 | 35.64 | 42.07 |
| Failure Atomicity | PASS | 3 | 2 | 1 | 38.57 | 38.69 | 27.99 |
| Race Update | PASS | 22 | 22 | 0 | 140.8 | 222.33 | 75.84 |

## ACID Verdict

### Atomicity — PASS
- Task 3: Full CRUD cycle succeeded atomically
- Task 4: Invalid create left zero partial data
- Task 5: Each concurrent write was all-or-nothing

### Consistency — PASS
- Task 3: Read-back matched submitted update exactly
- Task 5: Final doc state matched one valid submission
- No corrupted records detected in any scenario

### Isolation — PASS
- Task 1-B: Unauthorized users fully blocked
- Task 2: 15 concurrent readers with no interference
- Task 5: 20 concurrent writers — no corruption

### Durability — PASS
- Task 3: Created data was immediately readable
- Task 4: Existing data survived failed operations
- Reports persisted to disk
