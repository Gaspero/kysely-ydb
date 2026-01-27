# kysely-ydb

Kysely dialects, plugins and other goodies for [YDB](https://ydb.tech/)

## Disclaimer
This repo is currently WIP (Work in process) and thus is not production ready.

For now on, it only provides capabilities of basic select queries against YDB and does not support some dialect-specific expressions.

Types are mapped in accordance with @ydbjs/value [implementation](https://github.com/ydb-platform/ydb-js-sdk/blob/main/packages/value/README.md#type-conversion-details).

## Installation
```
npm i kysely-ydb
```

## Example usage
```typescript
import { Driver } from '@ydbjs/core'
import { Kysely } from 'kysely'
import { YdbDialect } from 'kysely-ydb'

interface UserTable {
  id: string
  name?: string
  email?: string
  email_verified?: Date
  image?: string
}

interface Database {
  users: UserTable
}

const YDB_ENDPOINT = 'grpcs://localhost:2135/local'

const driver = new Driver(YDB_ENDPOINT)

const db = new Kysely<Database>({
  dialect: new YdbDialect({
    driver,
  }),
})

export async function run() {
  const results = await db.selectFrom('users').select(['id', 'email', 'email_verified']).execute()
  console.log(results)
}

run()
```
