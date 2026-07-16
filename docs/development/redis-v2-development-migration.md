# Redis v2 development reset

The architecture refactor changes TaskForge's development key prefix from
`taskforge:` to `taskforge:v2:` and changes the task wire model from
`max_attempts` to `max_deliveries`. Mixed-version operation and rolling
migration are unsupported because TaskForge has no released storage
compatibility commitment.

The v2 broker, task-state store, DLQ, and scheduler do not read or mutate keys
from the old prefix. Republish any development tasks that must be retained.

To remove only pre-refactor TaskForge keys from the selected development Redis
database, inspect the keys first:

```bash
redis-cli --scan --pattern 'taskforge:*' |
  awk '$0 !~ /^taskforge:v2:/'
```

After confirming the connection and database are correct, delete that exact
set without flushing unrelated data:

```bash
redis-cli --scan --pattern 'taskforge:*' |
  awk '$0 !~ /^taskforge:v2:/' |
  xargs -r redis-cli UNLINK
```

Pass the same Redis connection flags to both `redis-cli` invocations when the
development instance is not the local default. Never use a database-wide flush
for a shared Redis database.
