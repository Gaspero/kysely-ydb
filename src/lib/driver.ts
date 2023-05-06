import {
  DatabaseConnection,
  Driver,
  QueryResult,
  RootOperationNode,
} from 'kysely';
import { TypedData, Ydb, Driver as YdbSdkDriver } from 'ydb-sdk';

import { YdbDialectConfig } from './dialect-config';
import { executeYdbQueryWithSession, IQueryParams } from './execute-query';
import { extendStackTrace, freeze, isFunction } from './utils';

export class YdbDriver implements Driver {
  readonly #config: YdbDialectConfig;

  #driver?: YdbSdkDriver;
  #connection?: DatabaseConnection;

  constructor(config: YdbDialectConfig) {
    this.#config = freeze({ ...config });
  }

  async init(): Promise<void> {
    this.#driver = isFunction(this.#config.driver)
      ? await this.#config.driver()
      : this.#config.driver;

    const timeout = 5000;
    if (!(await this.#driver.ready(timeout))) {
      throw Error(`Driver has not become ready in ${timeout}ms!`);
    }

    this.#connection = new YdbConnection(this.#driver);

    if (this.#config.onCreateConnection) {
      await this.#config.onCreateConnection(this.#connection);
    }
  }

  async acquireConnection(): Promise<DatabaseConnection> {
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    return this.#connection!;
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  async beginTransaction(_connection: DatabaseConnection): Promise<void> {
    throw new Error('Not Implemented');
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  async commitTransaction(_connection: DatabaseConnection): Promise<void> {
    throw new Error('Not Implemented');
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  async rollbackTransaction(_connection: DatabaseConnection): Promise<void> {
    throw new Error('Not Implemented');
  }

  async releaseConnection(): Promise<void> {
    // not implemented
  }

  async destroy(): Promise<void> {
    this.#driver?.destroy();
  }
}

interface YdbCompiledQuery {
  readonly query: RootOperationNode;
  readonly sql: string;
  readonly parameters: ReadonlyArray<Ydb.ITypedValue>;
}

class YdbConnection implements DatabaseConnection {
  #driver: YdbSdkDriver;

  constructor(driver: YdbSdkDriver) {
    this.#driver = driver;
  }

  async executeQuery<O>(
    compiledQuery: YdbCompiledQuery
  ): Promise<QueryResult<O>> {
    try {
      const queryParameters: IQueryParams =
        convertQueryParametersArrayToRecords(compiledQuery.parameters);

      const postProcessedCompiledQuery = serializeQuery(compiledQuery);

      const { resultSets } = await executeYdbQueryWithSession(
        this.#driver,
        postProcessedCompiledQuery,
        queryParameters
      );

      const resultParsed = resultSets[0]
        ? TypedData.createNativeObjects(resultSets[0]).map(
            (item) => Object.assign({}, item) as O
          )
        : [];

      return {
        rows: resultParsed,
      };
    } catch (err) {
      throw extendStackTrace(err, new Error());
    }
  }

  // eslint-disable-next-line require-yield
  async *streamQuery<R>(): AsyncIterableIterator<QueryResult<R>> {
    throw new Error("YDB driver doesn't support streaming");
  }
}

export function serializeQuery(compiledQuery: YdbCompiledQuery): string {
  const { parameters, sql } = compiledQuery;

  if (!parameters.length) {
    return `${sql};`;
  }

  return [
    ...parameters.map(
      (parameter, index) =>
        `declare $p${index + 1} AS ${typeToString(parameter.type)}`
    ),
    sql,
    '',
  ].join(';\n');
}

export const primitiveTypeToValue: Record<number, string> = {
  [Ydb.Type.PrimitiveTypeId.BOOL]: 'Bool',
  [Ydb.Type.PrimitiveTypeId.INT8]: 'Int8',
  [Ydb.Type.PrimitiveTypeId.UINT8]: 'Uint8',
  [Ydb.Type.PrimitiveTypeId.INT16]: 'Unt16',
  [Ydb.Type.PrimitiveTypeId.UINT16]: 'Uint16',
  [Ydb.Type.PrimitiveTypeId.INT32]: 'Unt32',
  [Ydb.Type.PrimitiveTypeId.UINT32]: 'Uint32',
  [Ydb.Type.PrimitiveTypeId.INT64]: 'Int64',
  [Ydb.Type.PrimitiveTypeId.UINT64]: 'Uint64',
  [Ydb.Type.PrimitiveTypeId.FLOAT]: 'FloatV',
  [Ydb.Type.PrimitiveTypeId.DOUBLE]: 'Double',
  [Ydb.Type.PrimitiveTypeId.STRING]: 'String',
  [Ydb.Type.PrimitiveTypeId.UTF8]: 'Utf8',
  [Ydb.Type.PrimitiveTypeId.YSON]: 'Yson',
  [Ydb.Type.PrimitiveTypeId.JSON]: 'Json',
  [Ydb.Type.PrimitiveTypeId.JSON_DOCUMENT]: 'JsonDocument',
  [Ydb.Type.PrimitiveTypeId.DYNUMBER]: 'DyNumber',
  [Ydb.Type.PrimitiveTypeId.DATE]: 'Date',
  [Ydb.Type.PrimitiveTypeId.DATETIME]: 'Datetime',
  [Ydb.Type.PrimitiveTypeId.TIMESTAMP]: 'Timestamp',
  [Ydb.Type.PrimitiveTypeId.INTERVAL]: 'Interval',
  [Ydb.Type.PrimitiveTypeId.TZ_DATE]: 'TzDate',
  [Ydb.Type.PrimitiveTypeId.TZ_DATETIME]: 'TzDateTime',
  [Ydb.Type.PrimitiveTypeId.TZ_TIMESTAMP]: 'TzTimestamp',
};

function typeToString(type: Ydb.IType | null | undefined): string {
  if (!type) {
    throw new Error('Type is empty');
  } else if (type.typeId) {
    return primitiveTypeToValue[type.typeId];
  } else if (type.decimalType) {
    throw new Error('decimalType not supported');
  } else if (type.optionalType) {
    throw new Error('optionalType not supported');
  } else if (type.listType) {
    throw new Error('listType not supported');
  } else if (type.tupleType) {
    throw new Error('tupleType not supported');
  } else if (type.structType) {
    throw new Error('structType not supported');
  } else if (type.dictType) {
    throw new Error('dictType not supported');
  } else if (type.variantType) {
    // nested structs here
    throw new Error('variantType not supported');
    // } else if (type.voidType === NullValue.NULL_VALUE) {
    //   throw new Error('voidType not supported')
  } else {
    throw new Error(`Unknown type ${JSON.stringify(type)}`);
  }
}

function convertQueryParametersArrayToRecords(
  queryParametersArray: ReadonlyArray<Ydb.ITypedValue>
): IQueryParams {
  const result: IQueryParams = {};

  for (let i = 0; i < queryParametersArray.length; i++) {
    result['$p' + (i + 1)] = queryParametersArray[i];
  }

  return result;
}
