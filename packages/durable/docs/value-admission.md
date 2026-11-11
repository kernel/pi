# Value admission

`Harness.open()` and `createSession()` accept an optional `validateValue(value)` callback. It is a pure, synchronous check of candidate JSON records, document values and deltas. It must not mutate its input or call the Session.

A thrown error rejects the transaction before storage or publication. Document drafts roll back and the Session remains usable. Task phase commits receive the rejection through the existing task-failure path rather than replaying a failed storage commit.

The check includes complete changed documents, even when storage would write only a delta, and copied document values. This prevents a series of small writes from constructing a document whose next base checkpoint exceeds the policy. If individually valid deferred changes fold into an invalid delta, storage writes their already-validated full value instead.

Actual storage failures still fail the Session. Keep validation errors bounded and content-free so reporting a rejected value does not itself exceed the admission policy. Validation must permit the scheduler's lifecycle records and bounded failure outcomes; rejecting those prevents safe task settlement.
