# Pulsar

Pulsar is a Node.js CLI that generates large volumes of fake user data and writes the result to one of several targets:

- `csv`
- `sqlite`
- `mysql`
- `postgresql`
- `mongodb`

It is useful for:

- seeding local development databases
- load and throughput experiments
- testing import pipelines
- generating demo datasets
- quickly creating a portable CSV or SQLite sample

The generated dataset includes these fields:

- `first_name`
- `last_name`
- `email`
- `phone`
- `address`
- `city`
- `country`
- `birth_date`
- `signup_date`
- `account_balance`

## How It Works

Pulsar resolves configuration from environment variables and built-in defaults, then creates batches of fake rows in worker threads and sends each batch to the selected writer.

If you launch the app without any environment variables, it immediately runs with these built-in defaults:

- target: `csv`
- total rows: `100000`
- batch size: `1000`
- worker count: `CPU cores - 1`
- CSV file: `pulsar.csv`

In that case, Pulsar does not prompt first. It starts generating data right away and writes to `pulsar.csv`.

Default behavior:

- target: `csv`
- total rows: `100000`
- batch size: `1000`
- worker count: `CPU cores - 1`
- DB connection count: `10`
- default database name: `pulsar`
- default table or collection name: `users`
- default CSV file: `pulsar.csv`
- default SQLite file: `pulsar.sqlite`

## Requirements

- Node.js 18+ recommended
- npm
- optional local or remote database only if you want to write to `mysql`, `postgresql`, or `mongodb`

## Installation

Clone the project and install dependencies:

```bash
git clone https://github.com/robeltila/Pulsar.git
cd Pulsar
npm install
```

## Default Usage

```bash
npm start
```

With no environment variables set, this command starts immediately and uses the built-in defaults.

Equivalent behavior:

```bash
TARGET=csv TOTAL_ROWS=100000 BATCH_SIZE=1000 CSV_FILE=pulsar.csv npm start
```

## Environment Variable Usage

You can override the built-in defaults with environment variables.

Supported variables:

- `TARGET`
- `TOTAL_ROWS`
- `BATCH_SIZE`
- `WORKER_COUNT`
- `CONNECTION_COUNT`
- `DB_NAME`
- `TABLE_NAME`
- `DB_URL`
- `DB_USER`
- `DB_PASSWORD`
- `CSV_FILE`
- `SQLITE_FILE`

## Targets

### CSV

Best when you want a portable file that can be opened in spreadsheet tools or imported elsewhere.

Example output file:

- `pulsar.csv`

### SQLite

Best when you want a zero-setup local database file.

Example output file:

- `pulsar.sqlite`

### MySQL

Best when your app already uses MySQL or MariaDB-style workflows.

Pulsar creates the database if needed and writes into the configured table.

### PostgreSQL

Best when your stack uses PostgreSQL and you want a seeded dataset quickly.

Pulsar creates the database if needed and writes into the configured table.

### MongoDB

Best when you want a document database dataset.

Pulsar creates documents in the configured collection.

## Platform Examples

### Linux Example

Generate a CSV file with explicit settings:

```bash
TARGET=csv \
TOTAL_ROWS=50000 \
BATCH_SIZE=1000 \
WORKER_COUNT=4 \
CSV_FILE=linux-users.csv \
npm start
```

Generate a PostgreSQL dataset:

```bash
TARGET=postgresql \
TOTAL_ROWS=25000 \
BATCH_SIZE=500 \
WORKER_COUNT=4 \
CONNECTION_COUNT=8 \
DB_NAME=pulsar \
TABLE_NAME=users \
DB_URL=postgresql://localhost:5432/pulsar \
DB_USER=postgres \
DB_PASSWORD=postgres \
npm start
```

### macOS Example

Generate a SQLite file:

```bash
TARGET=sqlite \
TOTAL_ROWS=20000 \
BATCH_SIZE=1000 \
WORKER_COUNT=4 \
TABLE_NAME=users \
SQLITE_FILE=mac-demo.sqlite \
npm start
```

Generate a MongoDB dataset:

```bash
TARGET=mongodb \
TOTAL_ROWS=30000 \
BATCH_SIZE=1000 \
WORKER_COUNT=4 \
CONNECTION_COUNT=10 \
DB_NAME=pulsar \
TABLE_NAME=users \
DB_URL=mongodb://127.0.0.1:27017 \
npm start
```

### Windows Example

In PowerShell, set environment variables like this:

```powershell
$env:TARGET="csv"
$env:TOTAL_ROWS="15000"
$env:BATCH_SIZE="500"
$env:WORKER_COUNT="4"
$env:CSV_FILE="windows-users.csv"
npm start
```

MySQL example in PowerShell:

```powershell
$env:TARGET="mysql"
$env:TOTAL_ROWS="25000"
$env:BATCH_SIZE="1000"
$env:WORKER_COUNT="4"
$env:CONNECTION_COUNT="8"
$env:DB_NAME="pulsar"
$env:TABLE_NAME="users"
$env:DB_URL="mysql://localhost:3306/pulsar"
$env:DB_USER="root"
$env:DB_PASSWORD="root"
npm start
```

If you prefer Command Prompt:

```cmd
set TARGET=sqlite
set TOTAL_ROWS=10000
set BATCH_SIZE=500
set WORKER_COUNT=4
set SQLITE_FILE=windows-demo.sqlite
npm start
```

## Example Workflows

### 1. Quick local file export

Use CSV when you want a flat file:

```bash
TARGET=csv CSV_FILE=users.csv npm start
```

### 2. Portable local database

Use SQLite when you want a single-file database:

```bash
TARGET=sqlite SQLITE_FILE=users.sqlite TABLE_NAME=users npm start
```

### 3. Seed a local PostgreSQL instance

```bash
TARGET=postgresql \
DB_URL=postgresql://localhost:5432/pulsar \
DB_USER=postgres \
DB_PASSWORD=postgres \
DB_NAME=pulsar \
TABLE_NAME=users \
npm start
```

### 4. Seed a local MySQL instance

```bash
TARGET=mysql \
DB_URL=mysql://localhost:3306/pulsar \
DB_USER=root \
DB_PASSWORD=root \
DB_NAME=pulsar \
TABLE_NAME=users \
npm start
```

### 5. Seed a local MongoDB instance

```bash
TARGET=mongodb \
DB_URL=mongodb://127.0.0.1:27017 \
DB_NAME=pulsar \
TABLE_NAME=users \
npm start
```

## Notes On Credentials

- If the username and password are already included in `DB_URL`, Pulsar can read them from there.
- If they are not in the URL, Pulsar will prompt for them unless `DB_USER` and `DB_PASSWORD` are set.
- For `csv` and `sqlite`, no database credentials are required.

Examples:

```bash
DB_URL=postgresql://postgres:postgres@localhost:5432/pulsar npm start
```

```bash
DB_URL=mongodb://admin:secret@127.0.0.1:27017 npm start
```

## Performance Tips

- Increase `WORKER_COUNT` for faster generation on multi-core machines.
- Increase `BATCH_SIZE` to reduce write overhead.
- Tune `CONNECTION_COUNT` for database targets if your DB can handle more parallel writes.
- For quick demos, `csv` and `sqlite` are the easiest targets to start with.

## Troubleshooting

### Missing dependency error

Install dependencies:

```bash
npm install
```

### Database connection issues

Check:

- the database service is running
- `DB_URL` is correct
- `DB_USER` and `DB_PASSWORD` are correct
- the target DB port is reachable

### Windows PowerShell execution looks different from Bash

That is expected. Bash examples use inline environment variables, while PowerShell uses `$env:NAME="value"`.

## Project Scripts

Start the generator:

```bash
npm start
```

## License

ISC
