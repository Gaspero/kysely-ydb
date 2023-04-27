import { DialectAdapterBase, Kysely } from 'kysely';

export class YdbAdapter extends DialectAdapterBase {
  get supportsTransactionalDdl(): boolean {
    return false;
  }

  get supportsReturning(): boolean {
    return false;
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  async acquireMigrationLock(_db: Kysely<unknown>): Promise<void> {
    // not implemented
  }

  async releaseMigrationLock(): Promise<void> {
    // not implemented
  }
}
