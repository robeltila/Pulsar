# Pulsar Fake Data Generator

Run the interactive generator with:

```bash
npm start
```

The CLI asks where to write data:

- `csv`
- `sqlite`
- `mysql`
- `postgresql`
- `mongodb`

It also prompts for:

- total rows
- batch size
- worker count
- connection count when the target uses a DB pool
- database URL
- username and password when they are not already present in the URL or environment
- default database, table/collection, and file names

Environment variable fallbacks:

- `TOTAL_ROWS`
- `BATCH_SIZE`
- `WORKER_COUNT`
- `CONNECTION_COUNT`
- `DB_NAME`
- `TABLE_NAME`
- `OUTPUT_FILE`
- `SQLITE_FILE`
- `DATABASE_URL`
- `DB_USER`
- `DB_PASSWORD`
- target-specific URL and credential variables such as `MONGODB_URI`, `POSTGRES_URL`, or `MYSQL_URL`
