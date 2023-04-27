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
