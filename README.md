<p align="center">
  <img src="https://raw.githubusercontent.com/dbt-labs/dbt/ec7dee39f793aa4f7dd3dae37282cc87664813e4/etc/dbt-logo-full.svg" alt="dbt logo" width="500"/>
</p>

# dbt-ydb

**dbt-ydb** is a plugin for [dbt](https://www.getdbt.com/) that provides support for working with [YDB](https://ydb.tech). **dbt-ydb** adapter is in preview stage and does not currently support all dbt features. The sections below list the supported features and known limitations.

## Installation

To install plugin, execute the following command:

```bash
pip install dbt-ydb
```

## Supported features

- [x] Table materialization
- [x] View materialization
- [x] Seeds
- [x] Docs generate
- [x] Tests
- [x] Incremental materializations (`merge` and `microbatch` strategies)
- [x] Snapshots
- [x] Cross-database (dbt "utils") macros: `dateadd`, `datediff`, `date_trunc`, `last_day`, `hash`, `split_part`, `concat`, `length`, `position`, `right`, `replace`, `bool_or`, `any_value`, `safe_cast`, `cast_bool_to_text`, `escape_single_quotes`, `type_*`, `except`, `intersect`, `array_construct`, `array_append`, `array_concat` (YDB `List<T>`)

## Limitations

* `datediff` on sub-second dateparts (`microsecond`/`millisecond`) requires `Timestamp` inputs; `Date`/`Datetime` columns carry only second precision.
* `type_float` / `type_numeric` / `type_boolean` / `type_timestamp` macros are available, but casting arbitrary string literals to these types follows YQL rules (e.g. a `Double` column cannot be a primary key).

* `YDB` does not support CTE
* `YDB` requires a primary key to be specified for its tables. See the configuration section for instructions on how to set it.
* `source()` macro requires you to specify a `schema`. Use `/` if your source is in root folder.

## Usage

### Profile Configuration

To configure YDB connection, fill `profile.yml` file as below:

```
profile_name:
  target: dev
  outputs:
    dev:
      type: ydb
      host: [localhost] # YDB host
      port: [2136] # YDB port
      database: [/local] # YDB database
      schema: [<empty string>] # Optional subfolder for DBT models
      secure: [False] # If enabled, grpcs protocol will be used
      root_certificates_path: [<empty string>] # Optional path to root certificates file

      # Static Credentials
      username: [<empty string>]
      password: [<empty string>]

      # Access Token Credentials
      token: [<empty string>]

      # Service Account Credentials
      service_account_credentials_file: [<empty string>]
```

### Model Configuration

#### View

| Option | Description | Required | Default |
| ------ | ----------- | -------- | ------- |

#### Table

| Option | Description | Required | Default |
| ------ | ----------- | -------- | ------- |
| `primary_key` | Primary key expression to use during table creation | `yes` | |
| `store_type` | Type of table. Available options are `row` and `column` | `no` | `row` |
| `partition_by` | Columns for the `PARTITION BY <method> (...)` clause. Column-oriented tables only (`store_type='column'`) | `no` | |
| `partition_method` | Partitioning method for `partition_by`. Currently YDB supports only `hash` | `no` | `hash` |
| `auto_partitioning_by_size` | Enable automatic partitioning by size. Available options are `ENABLED` and `DISABLED` | `no` | |
| `auto_partitioning_by_load` | Enable automatic partitioning by load. Available options are `ENABLED` and `DISABLED` | `no` | |
| `auto_partitioning_partition_size_mb` | Partition size in megabytes for automatic partitioning | `no` | |
| `auto_partitioning_min_partitions_count` | Minimum number of partitions | `no` | |
| `auto_partitioning_max_partitions_count` | Maximum number of partitions | `no` | |
| `uniform_partitions` | Number of pre-created uniform partitions (`Uint32`/`Uint64` keys) | `no` | |
| `partition_at_keys` | Explicit partition boundary keys, e.g. `(100, 200, 300)` | `no` | |
| `ttl` | Time-to-live (TTL) expression for automatic data expiration | `no` | |

#### Incremental

| Option | Description | Required | Default |
| ------ | ----------- | -------- | ------- |
| `incremental_strategy` | Strategy of incremental materialization: `merge`, which writes the model's rows with `YDB`'s `UPSERT`, or [`microbatch`](#microbatch), which does the same one time window at a time | `no` | `default` (same as `merge`) |
| `primary_key` | Primary key expression to use during table creation | `yes` | |
| `store_type` | Type of table. Available options are `row` and `column` | `no` | `row` |
| `partition_by` | Columns for the `PARTITION BY <method> (...)` clause. Column-oriented tables only (`store_type='column'`) | `no` | |
| `partition_method` | Partitioning method for `partition_by`. Currently YDB supports only `hash` | `no` | `hash` |
| `auto_partitioning_by_size` | Enable automatic partitioning by size. Available options are `ENABLED` and `DISABLED` | `no` | |
| `auto_partitioning_by_load` | Enable automatic partitioning by load. Available options are `ENABLED` and `DISABLED` | `no` | |
| `auto_partitioning_partition_size_mb` | Partition size in megabytes for automatic partitioning | `no` | |
| `auto_partitioning_min_partitions_count` | Minimum number of partitions | `no` | |
| `auto_partitioning_max_partitions_count` | Maximum number of partitions | `no` | |
| `uniform_partitions` | Number of pre-created uniform partitions (`Uint32`/`Uint64` keys) | `no` | |
| `partition_at_keys` | Explicit partition boundary keys, e.g. `(100, 200, 300)` | `no` | |
| `ttl` | Time-to-live (TTL) expression for automatic data expiration | `no` | |
| `tmp_relation_type` | How the rows are staged for the `UPSERT`: as a `view` (the model query is read once, straight into the target) or as a `table` (the result set is materialized first, then copied) | `no` | `view` |
| `tmp_<partitioning or TTL option>` | Override one of the partitioning or TTL options above for the staging table when `tmp_relation_type='table'`; for example, `tmp_auto_partitioning_min_partitions_count` | `no` | Corresponding table option |
| `merge_sql_header` | SQL header for the `UPSERT` statement. Replaces `sql_header` for that statement only | `no` | value of `sql_header` |
| `tmp_sql_header` | SQL header for the statement that creates the temp relation. Replaces `sql_header` for that statement only | `no` | value of `sql_header` |

##### Microbatch

`incremental_strategy='microbatch'` builds the model one time window at a time instead
of in a single query, which is what makes a large backfill (and a re-run of one bad day
of it) fit into a database at all. It is configured with dbt's own
[microbatch](https://docs.getdbt.com/docs/build/incremental-microbatch) keys -- the
adapter adds no keys of its own:

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    primary_key='id',
    event_time='created_at',
    batch_size='day',
    begin=modules.datetime.datetime(2025, 1, 1)
) }}

select id, name, created_at
from {{ ref('events') }}
```

dbt cuts `[begin, now)` into batches of `batch_size`, and runs the model once per batch
with every input that declares an `event_time` filtered down to that batch's window. A
batch owns its window in the target, so the adapter clears the window and writes the
batch's rows into it:

```sql
delete from `schema/model`
where created_at >= Timestamp("2025-01-02T00:00:00Z")
  and created_at < Timestamp("2025-01-03T00:00:00Z");

upsert into `schema/model` select `id`, `name`, `created_at` from `schema/model__dbt_tmp_20250102`;
```

Both statements are one script, so YDB runs them as a single transaction. That is what
makes a batch re-runnable: rows that left the window since the last run disappear from
the target instead of lingering there, and a batch can be replayed as many times as
needed -- `dbt run --event-time-start 2025-01-02 --event-time-end 2025-01-04` rebuilds
just those two days.

Notes specific to YDB:

* `event_time` may be a `Date`, `Datetime` or `Timestamp` column; the window is compared
  against `Timestamp` literals, which YQL accepts for all three;
* `incremental_predicates`, if the model sets any, narrow the `DELETE` further. Write them
  against plain column names (`created_at > ...`): YQL takes no alias after `DELETE FROM`,
  so the `DBT_INTERNAL_DEST.` prefix dbt's docs use for other databases does not compile
  here.

##### Staging: view or table

On an incremental run the adapter first stages the model's result set and then
`UPSERT`s it into the target. By default the staging relation is a **view**, so the
model query is planned into the `UPSERT` itself and the data is written exactly once:

```sql
create view `schema/model__dbt_tmp` with (security_invoker = TRUE) as select ... ;
upsert into `schema/model` select `a`, `b` from `schema/model__dbt_tmp`;
```

Set `tmp_relation_type='table'` to go back to staging into a real table
(`create table ... as select`, then `upsert ... from` it). That costs one extra full
write plus a read of the same volume, but it reads the sources before the target is
touched, which is what you want if:

* the model query is non-deterministic or reads a source that keeps changing, and you
  would rather it be snapshotted before the write starts;
* the model reads `{{ this }}` and you do not want the read and the write of the target
  to happen inside one query;
* the single query that reads the sources and writes the target runs into transaction
  limits.

The staging table inherits the target's `WITH` options unless a matching `tmp_` option
is set. For example, to use 8 partitions for a column-oriented staging table while
keeping 16 for the target:

```yaml
models:
  my_project:
    my_incremental_model:
      +materialized: incremental
      +primary_key: id
      +store_type: column
      +tmp_relation_type: table
      +auto_partitioning_min_partitions_count: 16
      +tmp_auto_partitioning_min_partitions_count: 8
```

`tmp_` overrides apply to the `auto_partitioning_*`, `uniform_partitions`,
`partition_at_keys`, and `ttl` settings, but not to `primary_key`, `store_type`,
or `partition_by`.

Model contracts always stage into a table -- a view carries no column definitions to
assert the contract against.

View staging needs a cluster with `CREATE VIEW` support; where views are not enabled,
incremental models need `tmp_relation_type='table'`.

##### Per-statement SQL headers

Building a model takes more than one statement, and `sql_header` goes in front of every
one of them. Statements differ in what they do and how they are planned, so a header
that fits one of them is not necessarily valid for the next. `merge_sql_header` and
`tmp_sql_header` replace `sql_header` for their own statement; an empty string means
"no header here":

```sql
{{ config(
    materialized='incremental',
    unique_key='id',
    primary_key='id',
    sql_header='PRAGMA ydb.DisableBlockExecution = "true";',
    merge_sql_header='',
    tmp_sql_header=''
) }}
```

##### Example table configuration

```sql
{{ config(
    primary_key='id, created_at',
    store_type='row',
    auto_partitioning_by_size='ENABLED',
    auto_partitioning_partition_size_mb=256,
    ttl='Interval("P30D") on created_at'
) }}

select
    id,
    name,
    created_at
from {{ ref('source_table') }}
```

##### Example column-oriented table with partitioning

```sql
{{ config(
    primary_key='id',
    store_type='column',
    partition_by='id',
    auto_partitioning_min_partitions_count=4
) }}

select id, name, created_at from {{ ref('source_table') }}
```

#### Seed

| Option | Description | Required | Default |
| ------ | ----------- | -------- | ------- |
| `primary_key` | Primary key expression to use during table creation | `no` | The first column of CSV will be used as default. |
