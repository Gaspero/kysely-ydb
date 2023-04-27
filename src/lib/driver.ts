import { DatabaseConnection, Driver, QueryResult, RootOperationNode } from 'kysely'
import {TypedData, Ydb, Driver as YdbSdkDriver} from 'ydb-sdk'

import { YdbDialectConfig } from './dialect-config'
import { executeYdbQueryWithSession, IQueryParams } from './execute-query'
import { extendStackTrace, freeze, isFunction } from "./utils"


export class YdbDriver implements Driver {
  readonly #config: YdbDialectConfig
  
  #driver?: YdbSdkDriver
  #connection?: DatabaseConnection

  constructor(config: YdbDialectConfig) {
    this.#config = freeze({ ...config })
  }

  async init(): Promise<void> {

    this.#driver = isFunction(this.#config.driver)
      ? await this.#config.driver()
      : this.#config.driver


    const timeout = 5000;
    if (!await this.#driver.ready(timeout)) {
        throw Error(`Driver has not become ready in ${timeout}ms!`);
    }
    console.log('started driver')
    

    this.#connection = new YdbConnection(this.#driver)

    if (this.#config.onCreateConnection) {
      await this.#config.onCreateConnection(this.#connection)
    }

  }

  async acquireConnection(): Promise<DatabaseConnection> {
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    return this.#connection!
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  async beginTransaction(_connection: DatabaseConnection): Promise<void> {
    throw new Error('Not Implemented')
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  async commitTransaction(_connection: DatabaseConnection): Promise<void> {
    throw new Error('Not Implemented')
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  async rollbackTransaction(_connection: DatabaseConnection): Promise<void> {
    throw new Error('Not Implemented')
  }

  async releaseConnection(): Promise<void> {
    // not implemented
  }

  async destroy(): Promise<void> {
    this.#driver?.destroy()
  }
}

interface ITypedValueQueryParams {
  key: string;
  value: Ydb.ITypedValue;
}

interface YdbCompiledQuery {
  readonly query: RootOperationNode;
  readonly sql: string;
  readonly parameters: ReadonlyArray<ITypedValueQueryParams>;
}

class YdbConnection implements DatabaseConnection {
  #driver: YdbSdkDriver

  constructor(driver: YdbSdkDriver) {
    this.#driver = driver
  }

  async executeQuery<O>(compiledQuery: YdbCompiledQuery): Promise<QueryResult<O>> {
    try {
      const queryParameters: IQueryParams = compiledQuery.parameters.reduce((a, v) => ({ ...a, [v.key]: v.value}), {})

      const { resultSets } = await executeYdbQueryWithSession(
        this.#driver, 
        compiledQuery.sql,
        queryParameters,
        );

      const resultParsed = TypedData.createNativeObjects(resultSets[0]).map(item => Object.assign({}, item) as O)

      return {
        rows: resultParsed ?? [],
      }
    } catch (err) {
      throw extendStackTrace(err, new Error())
    }
  }

  // eslint-disable-next-line require-yield
  async *streamQuery<R>(): AsyncIterableIterator<QueryResult<R>> {
    throw new Error('YDB driver doesn\'t support streaming')
  }
}
