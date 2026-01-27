import type { Driver } from '@ydbjs/core'

/**
 * Config for the YDB dialect.
 */
export interface YdbDialectConfig {
  /**
   * A YDB Driver instance or a function that returns one.
   */
  driver: Driver | (() => Promise<Driver>)
}
