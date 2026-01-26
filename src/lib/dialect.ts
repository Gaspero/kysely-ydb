import type {
  DatabaseIntrospector,
  Dialect,
  DialectAdapter,
  Driver,
  Kysely,
  QueryCompiler,
} from 'kysely'

import type { YdbDialectConfig } from './dialect-config'
import { YdbAdapter } from './adapter'
import { YdbDriver } from './driver'
import { YdbIntrospector } from './introspector'
import { YdbQueryCompiler } from './query-compiler'

/**
 * YDB dialect which uses the [ydb-js-sdk](https://github.com/ydb-platform/ydb-js-sdk/tree/main) library.
 *
 * The constructor takes an instance of {@link YdbDialectConfig}.
 *
 * ```ts
 * import { Driver } from '@ydbjs/core'
 *
 * new YdbDialect({
 *   driver: new Driver(...)
 * })
 * ```
 */
export class YdbDialect implements Dialect {
  readonly #config: YdbDialectConfig

  constructor(config: YdbDialectConfig) {
    this.#config = config
  }

  createDriver(): Driver {
    return new YdbDriver(this.#config)
  }

  createQueryCompiler(): QueryCompiler {
    return new YdbQueryCompiler()
  }

  createAdapter(): DialectAdapter {
    return new YdbAdapter()
  }

  createIntrospector(_db: Kysely<unknown>): DatabaseIntrospector {
    return new YdbIntrospector()
  }
}
