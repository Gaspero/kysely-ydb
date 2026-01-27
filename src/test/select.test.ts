import type {
  Database,
} from './utils'
import { Driver } from '@ydbjs/core'
import { Kysely } from 'kysely'

import {
  afterAll,
  afterEach,
  beforeAll,
  beforeEach,
  describe,
  expect,
  it,
} from 'vitest'
import { YdbDialect } from '../lib/dialect'
import {
  clearDatabase,
  destroyTest,
  driverSettings,
  initTest,
  insertDefaultDataSet,
  YDB_ENDPOINT,
} from './utils'

const driver = new Driver(YDB_ENDPOINT, driverSettings)

beforeAll(async () => {
  await initTest(driver)
})

beforeEach(async () => {
  await insertDefaultDataSet(driver)
})

afterEach(async () => {
  await clearDatabase(driver)
})

afterAll(async () => {
  await destroyTest(driver)
})

describe('select', () => {
  const db = new Kysely<Database>({
    dialect: new YdbDialect({
      driver,
    }),
  })

  it('select all', async () => {
    const data = await db.selectFrom('series').selectAll().execute()

    expect(data).toEqual([
      {
        series_id: 1,
        title: 'IT Crowd',
        release_date: new Date('2006-02-03'),
        series_info:
          'The IT Crowd is a British sitcom produced by Channel 4, written by Graham Linehan, produced by '
          + 'Ash Atalla and starring Chris O\'Dowd, Richard Ayoade, Katherine Parkinson, and Matt Berry.',
        is_closed: true,
      },
      {
        series_id: 2,
        title: 'Silicon Valley',
        release_date: new Date('2014-04-06'),
        series_info:
          'Silicon Valley is an American comedy television series created by Mike Judge, John Altschuler and '
          + 'Dave Krinsky. The series focuses on five young men who founded a startup company in Silicon Valley.',
        is_closed: true,
      },
    ])
  })

  it('select all with order by', async () => {
    const data = await db
      .selectFrom('series')
      .selectAll()
      .orderBy('series_id', 'desc')
      .execute()

    expect(data).toEqual([
      {
        series_id: 2,
        title: 'Silicon Valley',
        release_date: new Date('2014-04-06'),
        series_info:
          'Silicon Valley is an American comedy television series created by Mike Judge, John Altschuler and '
          + 'Dave Krinsky. The series focuses on five young men who founded a startup company in Silicon Valley.',
        is_closed: true,
      },
      {
        series_id: 1,
        title: 'IT Crowd',
        release_date: new Date('2006-02-03'),
        series_info:
          'The IT Crowd is a British sitcom produced by Channel 4, written by Graham Linehan, produced by '
          + 'Ash Atalla and starring Chris O\'Dowd, Richard Ayoade, Katherine Parkinson, and Matt Berry.',
        is_closed: true,
      },
    ])
  })

  it('select all with limit', async () => {
    const data = await db.selectFrom('series').selectAll().limit(1).execute()

    expect(data).toEqual([
      {
        series_id: 1,
        title: 'IT Crowd',
        release_date: new Date('2006-02-03'),
        series_info:
          'The IT Crowd is a British sitcom produced by Channel 4, written by Graham Linehan, produced by '
          + 'Ash Atalla and starring Chris O\'Dowd, Richard Ayoade, Katherine Parkinson, and Matt Berry.',
        is_closed: true,
      },
    ])
  })

  it('select one', async () => {
    const data = await db
      .selectFrom('series')
      .select(['series_id', 'title', 'release_date'])
      .where('series_id', '=', 1)
      .executeTakeFirst()

    expect(data).toEqual({
      series_id: 1,
      title: 'IT Crowd',
      release_date: new Date('2006-02-03'),
    })
  })

  it('select with join', async () => {
    const data = await db
      .selectFrom('series')
      .innerJoin('seasons', 'seasons.series_id', 'series.series_id')
      .select([
        'series.series_id as series_id',
        'series.title as title',
        'series.release_date as release_date',
        'seasons.season_id as season_id',
        'seasons.title as season_title',
      ])
      .where('series.series_id', '=', 1)
      .execute()

    expect(data).toEqual([
      {
        series_id: 1,
        title: 'IT Crowd',
        release_date: new Date('2006-02-03'),
        season_id: 1,
        season_title: 'Season 1',
      },
      {
        series_id: 1,
        title: 'IT Crowd',
        release_date: new Date('2006-02-03'),
        season_id: 2,
        season_title: 'Season 2',
      },
      {
        series_id: 1,
        title: 'IT Crowd',
        release_date: new Date('2006-02-03'),
        season_id: 3,
        season_title: 'Season 3',
      },
      {
        series_id: 1,
        title: 'IT Crowd',
        release_date: new Date('2006-02-03'),
        season_id: 4,
        season_title: 'Season 4',
      },
    ])
  })
})
