<p align="center">
  <img src="https://raw.githubusercontent.com/dbt-labs/dbt/ec7dee39f793aa4f7dd3dae37282cc87664813e4/etc/dbt-logo-full.svg" alt="dbt logo" width="500"/>
</p>

# dbt-ydb

**dbt-ydb** is a plugin for [dbt](https://www.getdbt.com/) that provides support for working with [YDB](https://ydb.tech).

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
- [ ] Incremental materializations
- [ ] Snapshots

## Limitations

* `YDB` does not support CTE
* `YDB` requires a primary key to be specified for its tables. See the configuration section for instructions on how to set it.

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
| `auto_partitioning_by_size` | Enable automatic partitioning by size. Available options are `ENABLED` and `DISABLED` | `no` | |
| `auto_partitioning_partition_size_mb` | Partition size in megabytes for automatic partitioning | `no` | |
| `ttl` | Time-to-live (TTL) expression for automatic data expiration | `no` | |

#### Incremental

| Option | Description | Required | Default |
| ------ | ----------- | -------- | ------- |
| `incremental_strategy` | Strategy of incremental materialization. Current adapter supports only `merge` strategy, which will use `YDB`'s `UPSERT` operation. | `no` | `default` |
| `primary_key` | Primary key expression to use during table creation | `yes` | |
| `store_type` | Type of table. Available options are `row` and `column` | `no` | `row` |
| `auto_partitioning_by_size` | Enable automatic partitioning by size. Available options are `ENABLED` and `DISABLED` | `no` | |
| `auto_partitioning_partition_size_mb` | Partition size in megabytes for automatic partitioning | `no` | |
| `ttl` | Time-to-live (TTL) expression for automatic data expiration | `no` | |

##### Example table configuration

```sql
{{ config(
    primary_key='id, created_at',
    store_type='column',
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

#### Seed

| Option | Description | Required | Default |
| ------ | ----------- | -------- | ------- |
| `primary_key` | Primary key expression to use during table creation | `no` | The first column of CSV will be used as default. |
