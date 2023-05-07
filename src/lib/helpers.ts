import { RawBuilder, sql } from 'kysely';
import { type Ydb } from 'ydb-sdk';

export function typedParam<T>(value: Ydb.ITypedValue): RawBuilder<T> {
  return sql`${value}`;
}
