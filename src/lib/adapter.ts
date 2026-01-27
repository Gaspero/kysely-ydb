import type { Kysely, MigrationLockOptions } from 'kysely'
import { DialectAdapterBase } from 'kysely'

export class YdbAdapter extends DialectAdapterBase {
  get supportsTransactionalDdl(): boolean {
    return false
  }

  get supportsReturning(): boolean {
    return false
  }

  acquireMigrationLock(_db: Kysely<any>, _options: MigrationLockOptions): Promise<void> {
    throw new Error('Method not implemented.')
  }

  releaseMigrationLock(_db: Kysely<any>, _options: MigrationLockOptions): Promise<void> {
    throw new Error('Method not implemented.')
  }
}
