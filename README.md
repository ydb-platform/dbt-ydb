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
- [ ] TBD

## Usage

### Configuration

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