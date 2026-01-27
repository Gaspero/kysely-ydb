/* eslint-disable ts/explicit-function-return-type */
/* eslint-disable no-console */
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
