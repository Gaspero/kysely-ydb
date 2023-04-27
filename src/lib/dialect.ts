import {
  DatabaseIntrospector,
  Dialect,
  DialectAdapter,
  Driver,
  Kysely,
  QueryCompiler,
} from 'kysely';

import { YdbAdapter } from './adapter';
import { YdbDialectConfig } from './dialect-config';
import { YdbDriver } from './driver';
import { YdbIntrospector } from './introspector';
import { YdbQueryCompiler } from './query-compiler';

export class YdbDialect implements Dialect {
  readonly #config: YdbDialectConfig;

  constructor(config: YdbDialectConfig) {
    this.#config = config;
  }

  createDriver(): Driver {
    return new YdbDriver(this.#config);
  }

  createQueryCompiler(): QueryCompiler {
    return new YdbQueryCompiler();
  }

  createAdapter(): DialectAdapter {
    return new YdbAdapter();
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  createIntrospector(_db: Kysely<unknown>): DatabaseIntrospector {
    return new YdbIntrospector();
  }
}
