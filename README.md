# Idempotent Background Transaction Processing Service

A lightweight Java 17 / Spring Boot implementation for the FDE L2 assessment. It demonstrates transaction validation, idempotency, deterministic sequence ordering within a submitted batch, transient retry simulation, status reporting, and tests.

## Requirements
- Java 17+
- Maven 3.9+ (or use an IDE with Maven support)

## Run
```bash
mvn clean test
mvn spring-boot:run
```
Service starts at `http://localhost:8080`.

## Try the API
Submit the sample batch:
```bash
curl -X POST http://localhost:8080/api/transactions/batch \
  -H 'Content-Type: application/json' \
  --data-binary @sample/transactions.json
```
Inspect recorded results and balances:
```bash
curl http://localhost:8080/api/transactions
curl http://localhost:8080/api/balances
```

The `REQ-5` request intentionally fails once and succeeds on the next attempt. This is a deterministic test hook, not a production failure-injection mechanism.

## Architecture
- `TransactionController`: thin HTTP adapter.
- `TransactionProcessor`: validates business rules, orders batch records, detects replays, applies balance changes, retries transient errors, and produces a status summary.
- `TransactionRequest`, `TransactionResult`, `ProcessingReport`: request and response models.
- `TransactionProcessorTest`: unit tests for duplicate requests, out-of-order batch input, invalid amounts, retry safety, and sequence gaps.

## Idempotency and duplicate detection
`transactionId` is the business idempotency key. A transaction that has already reached `PROCESSED` returns `DUPLICATE` and does not apply another balance change. Request IDs are also tracked for diagnostic duplicate detection.

## Ordering strategy
The service sorts records in each submitted batch by `accountId` and `sequenceNumber` before applying them. Sequence numbers are scoped per account. A gap relative to the next expected sequence is marked `PENDING`, and an already-passed sequence is treated as stale/duplicate. This is intentionally simple for the exercise.

When a missing sequence arrives in a later HTTP call, the processor attempts to drain contiguous pending records for that account. Pending storage is still in memory, so it is lost on restart. Production hardening should use a durable pending queue and atomically claim the next sequence while draining contiguous pending events.

## Status lifecycle
`RECEIVED` → `PROCESSING` → `PROCESSED`, with `RETRY_PENDING` during a retry; invalid/exhausted requests become `FAILED`; replays become `DUPLICATE`; sequence gaps become `PENDING`.

The in-memory implementation returns final batch outcomes; it does not expose intermediate states as a durable history.

## Consistency and production limitations
This assessment implementation uses in-memory maps and synchronizes batch processing to keep the demo straightforward. State is lost on restart and this is not horizontally scalable. For production:
1. Persist transaction state, idempotency keys, account sequence/watermark, ledger entries, and balances in a relational database.
2. Use a unique constraint on `transaction_id` (and/or idempotency key) and commit the ledger entry, balance mutation, and processed status in one database transaction.
3. Use optimistic locking or account-level locking to prevent concurrent balance races.
4. Use a durable queue with at-least-once delivery; assume duplicates will occur.
5. Use exponential backoff with jitter, retry classification, max attempts, and a dead-letter queue for exhausted messages.
6. Add metrics for received/processed/duplicate/pending/failed/retried, latency, queue age, and retry exhaustion.
7. Add correlation IDs, structured logs, authentication/authorization, rate limits, and payload-size limits. Do not log full customer payloads.
8. Define reversal semantics explicitly: in production, a reversal should reference the original transaction and be idempotent independently.

## AI-assisted development
AI assistance was used to accelerate the initial scaffold, test-case design, and documentation. The implementation should be reviewed by the candidate: run the tests, inspect retry and ordering logic, and be ready to explain why in-memory state is not production safe. Do not claim tests passed unless you ran them locally.

## Suggested demonstration
1. Run tests.
2. Submit the sample batch and show that `ACC-201` ends with 85 (`100 - 40 + 25`).
3. Submit a processed transaction again with a different `requestId`; verify status `DUPLICATE` and unchanged balance.
4. Show the invalid negative amount isolated as `FAILED`.
5. Show `REQ-5` succeeds on attempt 2.
6. Submit sequence 2 without sequence 1 on a fresh account and show `PENDING`.
7. Explain durable persistence, atomicity, concurrency, recovery, and observability as production next steps.
