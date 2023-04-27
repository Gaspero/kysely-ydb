import { DatabaseConnection } from 'kysely'
import { type Driver } from 'ydb-sdk'

/**
 * Config for the YDB dialect.
 */
export interface YdbDialectConfig {
  /**
   * A YDB Driver instance or a function that returns one.
   */
  driver: Driver | (() => Promise<Driver>)

  /**
   * Called once for each created connection.
   */
  onCreateConnection?: (connection: DatabaseConnection) => Promise<void>
}
