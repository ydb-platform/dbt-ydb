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
- [x] Incremental materializations (`merge` strategy only)
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
| `incremental_strategy` | Strategy of incremental materialization. Current adapter supports only `merge` strategy, which will use `YDB`'s `UPSERT` operation. | `no` | `default` |
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
