import type { Driver as YdbSdkDriver } from '@ydbjs/core'
import type { Type, Value } from '@ydbjs/value'
import type {
  DatabaseConnection,
  Driver,
  QueryCompiler,
  QueryResult,
  RootOperationNode,
  TransactionSettings,
} from 'kysely'

import type { YdbDialectConfig } from './dialect-config'
import { query } from '@ydbjs/query'
import { extendStackTrace, freeze, isFunction } from './utils'

export class YdbDriver implements Driver {
  readonly #config: YdbDialectConfig

  #driver?: YdbSdkDriver
  #connection?: YdbConnection

  constructor(config: YdbDialectConfig) {
    this.#config = freeze({ ...config })
  }

  acquireConnection(): Promise<DatabaseConnection> {
    return Promise.resolve(this.#connection!)
  }

  beginTransaction(_connection: DatabaseConnection, _settings: TransactionSettings): Promise<void> {
    throw new Error('Method not implemented.')
  }

  commitTransaction(_connection: DatabaseConnection): Promise<void> {
    throw new Error('Method not implemented.')
  }

  rollbackTransaction(_connection: DatabaseConnection): Promise<void> {
    throw new Error('Method not implemented.')
  }

  savepoint?(_connection: DatabaseConnection, _savepointName: string, _compileQuery: QueryCompiler['compileQuery']): Promise<void> {
    throw new Error('Method not implemented.')
  }

  rollbackToSavepoint?(_connection: DatabaseConnection, _savepointName: string, _compileQuery: QueryCompiler['compileQuery']): Promise<void> {
    throw new Error('Method not implemented.')
  }

  releaseSavepoint?(_connection: DatabaseConnection, _savepointName: string, _compileQuery: QueryCompiler['compileQuery']): Promise<void> {
    throw new Error('Method not implemented.')
  }

  releaseConnection(_connection: DatabaseConnection): Promise<void> {
    return Promise.resolve()
    // throw new Error('Method not implemented.')
  }

  async init(): Promise<void> {
    this.#driver = isFunction(this.#config.driver)
      ? await this.#config.driver()
      : this.#config.driver

    await this.#driver.ready()

    this.#connection = new YdbConnection(this.#driver)

    // if (this.#config.onCreateConnection) {
    //   await this.#config.onCreateConnection(this.#connection);
    // }
  }

  async destroy(): Promise<void> {
    this.#driver?.close()
  }
}

interface YdbCompiledQuery {
  readonly query: RootOperationNode
  readonly sql: string
  readonly parameters: ReadonlyArray<unknown>
}

class YdbConnection implements DatabaseConnection {
  #driver: YdbSdkDriver

  constructor(driver: YdbSdkDriver) {
    this.#driver = driver
  }

  async executeQuery<O>(
    compiledQuery: YdbCompiledQuery,
  ): Promise<QueryResult<O>> {
    try {
      const { sql, parameters } = compiledQuery
      const queryClient = query(this.#driver)
      const queryString = queryClient<O[]>(sql)
      for (const i in parameters) {
        queryString.parameter(`$p${i}`, parameters[i] as Value<Type>)
      }

      const [result] = await queryString

      return {
        rows: result,
      }
    }
    catch (err) {
      throw extendStackTrace(err, new Error('ne schmogla'))
    }
  }

  async* streamQuery<R>(): AsyncIterableIterator<QueryResult<R>> {
    throw new Error('YDB driver doesn\'t support streaming')
  }
}
