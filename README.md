# kysely-ydb

Kysely dialects, plugins and other goodies for [YDB](https://ydb.tech/)

## Disclaimer
This repo is currently WIP (Work in process) and thus is not production ready.

For now on, it only provides capabilities of basic select queries against YDB and does not support some dialect-specific expressions.

## Instalation 
```
npm i kysely-ydb
```

## Example usage
```
import { Kysely } from 'kysely';
import {Driver, getSACredentialsFromJson, IamAuthService} from 'ydb-sdk';

import { YdbDialect } from 'kysely-ydb';

interface UserTable {
  id: string;
  name?: string;
  email?: string;
  email_verified?: Date;
  image?: string;
}

interface Database {
  users: UserTable
}

const YDB_ENDPOINT = 'grpcs://ydb.serverless.yandexcloud.net:2135';
const YDB_DB = '/ru-central1/{YOUR_DB}';

const saCredentials = getSACredentialsFromJson('key.json');
const authService = new IamAuthService(saCredentials);
const driver = new Driver({
    endpoint: YDB_ENDPOINT,
    database: YDB_DB,
    authService: authService

});

const db = new Kysely<Database>({
  dialect: new YdbDialect({
    driver: driver
  }),
})

export async function run() {
  const results = await db.selectFrom('users').select(['id', 'email', 'email_verified']).execute()
  console.log(results)
}

run()

```
