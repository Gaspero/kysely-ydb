import {
  DatabaseIntrospector,
  DatabaseMetadata,
  DatabaseMetadataOptions,
  SchemaMetadata,
  TableMetadata,
} from 'kysely';

export function freeze<T>(obj: T): Readonly<T> {
  return Object.freeze(obj);
}

export class YdbIntrospector implements DatabaseIntrospector {
  async getSchemas(): Promise<SchemaMetadata[]> {
    // YDB doesn't support schemas.
    return [];
  }

  async getTables(
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    _options: DatabaseMetadataOptions = { withInternalKyselyTables: false }
  ): Promise<TableMetadata[]> {
    throw new Error('Not implemented');
  }

  async getMetadata(
    options?: DatabaseMetadataOptions
  ): Promise<DatabaseMetadata> {
    return {
      tables: await this.getTables(options),
    };
  }
}
