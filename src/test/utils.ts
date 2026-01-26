import type { Driver, DriverOptions } from '@ydbjs/core'
import fs from 'node:fs'
import path from 'node:path'
import { query } from '@ydbjs/query'

export const SYNTAX_V1 = '--!syntax_v1'

export const SERIES_TABLE = 'series'
export const SEASONS_TABLE = 'seasons'
export const EPISODES_TABLE = 'episodes'

export const YDB_ENDPOINT = 'grpcs://localhost:2135/local'

export interface Series {
  series_id: number
  title: string | null
  series_info: string | null
  release_date: Date | null
  is_closed: boolean | null
}

export interface Seasons {
  series_id: number
  season_id: number
  title: string | null
  first_aired: Date | null
  last_aired: Date | null
}

export interface Episodes {
  series_id: number
  season_id: number
  episode_id: number
  title: string | null
  air_date: Date | null
}

export interface Database {
  series: Series
  seasons: Seasons
  episodes: Episodes
}

// eslint-disable-next-line node/prefer-global/process
const ca = fs.readFileSync(path.join(process.cwd(), 'ydb_certs/ca.pem'))
// eslint-disable-next-line node/prefer-global/process
const cert = fs.readFileSync(path.join(process.cwd(), 'ydb_certs/cert.pem'))
// eslint-disable-next-line node/prefer-global/process
const key = fs.readFileSync(path.join(process.cwd(), 'ydb_certs/key.pem'))

export const driverSettings: DriverOptions = {
  secureOptions: { ca, cert, key },
}

/* ------------------------------------------------------------------ */
/* lifecycle                                                          */
/* ------------------------------------------------------------------ */

export async function initTest(driver: Driver): Promise<void> {
  await driver.ready()
  await dropTables(driver)
  await createTables(driver)
}

export async function destroyTest(driver: Driver): Promise<void> {
  await dropTables(driver)
  driver.close()
}

export async function clearDatabase(driver: Driver): Promise<void> {
  const sql = query(driver)

  await sql`DELETE FROM ${sql.identifier(SERIES_TABLE)}`
  await sql`DELETE FROM ${sql.identifier(SEASONS_TABLE)}`
  await sql`DELETE FROM ${sql.identifier(EPISODES_TABLE)}`
}

/* ------------------------------------------------------------------ */
/* schema                                                             */
/* ------------------------------------------------------------------ */

export async function createTables(driver: Driver): Promise<void> {
  const sql = query(driver)

  await sql`
    CREATE TABLE ${sql.identifier(SERIES_TABLE)} (
      series_id Int32,
      title Text,
      series_info Text,
      release_date Datetime,
      is_closed Bool,
      PRIMARY KEY (series_id)
    )
  `

  await sql`
    CREATE TABLE ${sql.identifier(SEASONS_TABLE)} (
      series_id Int32,
      season_id Int32,
      title Text,
      first_aired Datetime,
      last_aired Datetime,
      PRIMARY KEY (series_id, season_id)
    )
  `

  await sql`
    CREATE TABLE ${sql.identifier(EPISODES_TABLE)} (
      series_id Int32,
      season_id Int32,
      episode_id Int64,
      title Text,
      air_date Datetime,
      PRIMARY KEY (series_id, season_id, episode_id)
    )
  `
}

export async function dropTables(driver: Driver): Promise<void> {
  const sql = query(driver)

  await sql`DROP TABLE IF EXISTS ${sql.identifier(EPISODES_TABLE)}`
  await sql`DROP TABLE IF EXISTS ${sql.identifier(SEASONS_TABLE)}`
  await sql`DROP TABLE IF EXISTS ${sql.identifier(SERIES_TABLE)}`
}

/* ------------------------------------------------------------------ */
/* data                                                               */
/* ------------------------------------------------------------------ */

export function getSeriesData(): Series[] {
  return [
    {
      series_id: 1,
      title: 'IT Crowd',
      series_info:
        'The IT Crowd is a British sitcom produced by Channel 4, written by Graham Linehan, produced by Ash Atalla and starring Chris O\'Dowd, Richard Ayoade, Katherine Parkinson, and Matt Berry.',
      release_date: new Date('2006-02-03'),
      is_closed: true,
    },
    {
      series_id: 2,
      title: 'Silicon Valley',
      series_info:
        'Silicon Valley is an American comedy television series created by Mike Judge, John Altschuler and Dave Krinsky. The series focuses on five young men who founded a startup company in Silicon Valley.',
      release_date: new Date('2014-04-06'),
      is_closed: true,
    },
  ]
}

export function getSeasonsData(): Seasons[] {
  return [
    { series_id: 1, season_id: 1, title: 'Season 1', first_aired: new Date('2006-02-03'), last_aired: new Date('2006-03-03') },
    { series_id: 1, season_id: 2, title: 'Season 2', first_aired: new Date('2007-08-24'), last_aired: new Date('2007-09-28') },
    { series_id: 1, season_id: 3, title: 'Season 3', first_aired: new Date('2008-11-21'), last_aired: new Date('2008-12-26') },
    { series_id: 1, season_id: 4, title: 'Season 4', first_aired: new Date('2010-06-25'), last_aired: new Date('2010-07-30') },
    { series_id: 2, season_id: 1, title: 'Season 1', first_aired: new Date('2014-04-06'), last_aired: new Date('2014-06-01') },
    { series_id: 2, season_id: 2, title: 'Season 2', first_aired: new Date('2015-04-12'), last_aired: new Date('2015-06-14') },
    { series_id: 2, season_id: 3, title: 'Season 3', first_aired: new Date('2016-04-24'), last_aired: new Date('2016-06-26') },
    { series_id: 2, season_id: 4, title: 'Season 4', first_aired: new Date('2017-04-23'), last_aired: new Date('2017-06-25') },
    { series_id: 2, season_id: 5, title: 'Season 5', first_aired: new Date('2018-03-25'), last_aired: new Date('2018-05-13') },
  ]
}

export function getEpisodesData(): Episodes[] {
  return [
    { series_id: 1, season_id: 1, episode_id: 1, title: 'Yesterday’s Jam', air_date: new Date('2006-02-03') },
    { series_id: 1, season_id: 1, episode_id: 2, title: 'Calamity Jen', air_date: new Date('2006-02-03') },
    { series_id: 2, season_id: 1, episode_id: 1, title: 'Minimum Viable Product', air_date: new Date('2014-04-06') },
    { series_id: 1, season_id: 1, episode_id: 1, title: 'Yesterday\'s Jam', air_date: new Date('2006-02-03') },
    { series_id: 1, season_id: 1, episode_id: 2, title: 'Calamity Jen', air_date: new Date('2006-02-03') },
    { series_id: 1, season_id: 1, episode_id: 3, title: 'Fifty-Fifty', air_date: new Date('2006-02-10') },
    { series_id: 1, season_id: 1, episode_id: 4, title: 'The Red Door', air_date: new Date('2006-02-17') },
    { series_id: 1, season_id: 1, episode_id: 5, title: 'The Haunting of Bill Crouse', air_date: new Date('2006-02-24') },
    { series_id: 1, season_id: 1, episode_id: 6, title: 'Aunt Irma Visits', air_date: new Date('2006-03-03') },
    { series_id: 1, season_id: 2, episode_id: 1, title: 'The Work Outing', air_date: new Date('2006-08-24') },
    { series_id: 1, season_id: 2, episode_id: 2, title: 'Return of the Golden Child', air_date: new Date('2007-08-31') },
    { series_id: 1, season_id: 2, episode_id: 3, title: 'Moss and the German', air_date: new Date('2007-09-07') },
    { series_id: 1, season_id: 2, episode_id: 4, title: 'The Dinner Party', air_date: new Date('2007-09-14') },
    { series_id: 1, season_id: 2, episode_id: 5, title: 'Smoke and Mirrors', air_date: new Date('2007-09-21') },
    { series_id: 1, season_id: 2, episode_id: 6, title: 'Men Without Women', air_date: new Date('2007-09-28') },
    { series_id: 1, season_id: 3, episode_id: 1, title: 'From Hell', air_date: new Date('2008-11-21') },
    { series_id: 1, season_id: 3, episode_id: 2, title: 'Are We Not Men?', air_date: new Date('2008-11-28') },
    { series_id: 1, season_id: 3, episode_id: 3, title: 'Tramps Like Us', air_date: new Date('2008-12-05') },
    { series_id: 1, season_id: 3, episode_id: 4, title: 'The Speech', air_date: new Date('2008-12-12') },
    { series_id: 1, season_id: 3, episode_id: 5, title: 'Friendface', air_date: new Date('2008-12-19') },
    { series_id: 1, season_id: 3, episode_id: 6, title: 'Calendar Geeks', air_date: new Date('2008-12-26') },
    { series_id: 1, season_id: 4, episode_id: 1, title: 'Jen The Fredo', air_date: new Date('2010-06-25') },
    { series_id: 1, season_id: 4, episode_id: 2, title: 'The Final Countdown', air_date: new Date('2010-07-02') },
    { series_id: 1, season_id: 4, episode_id: 3, title: 'Something Happened', air_date: new Date('2010-07-09') },
    { series_id: 1, season_id: 4, episode_id: 4, title: 'Italian For Beginners', air_date: new Date('2010-07-16') },
    { series_id: 1, season_id: 4, episode_id: 5, title: 'Bad Boys', air_date: new Date('2010-07-23') },
    { series_id: 1, season_id: 4, episode_id: 6, title: 'Reynholm vs Reynholm', air_date: new Date('2010-07-30') },
    { series_id: 2, season_id: 1, episode_id: 1, title: 'Minimum Viable Product', air_date: new Date('2014-04-06') },
    { series_id: 2, season_id: 1, episode_id: 2, title: 'The Cap Table', air_date: new Date('2014-04-13') },
    { series_id: 2, season_id: 1, episode_id: 3, title: 'Articles of Incorporation', air_date: new Date('2014-04-20') },
    { series_id: 2, season_id: 1, episode_id: 4, title: 'Fiduciary Duties', air_date: new Date('2014-04-27') },
    { series_id: 2, season_id: 1, episode_id: 5, title: 'Signaling Risk', air_date: new Date('2014-05-04') },
    { series_id: 2, season_id: 1, episode_id: 6, title: 'Third Party Insourcing', air_date: new Date('2014-05-11') },
    { series_id: 2, season_id: 1, episode_id: 7, title: 'Proof of Concept', air_date: new Date('2014-05-18') },
    { series_id: 2, season_id: 1, episode_id: 8, title: 'Optimal Tip-to-Tip Efficiency', air_date: new Date('2014-06-01') },
    { series_id: 2, season_id: 2, episode_id: 1, title: 'Sand Hill Shuffle', air_date: new Date('2015-04-12') },
    { series_id: 2, season_id: 2, episode_id: 2, title: 'Runaway Devaluation', air_date: new Date('2015-04-19') },
    { series_id: 2, season_id: 2, episode_id: 3, title: 'Bad Money', air_date: new Date('2015-04-26') },
    { series_id: 2, season_id: 2, episode_id: 4, title: 'The Lady', air_date: new Date('2015-05-03') },
    { series_id: 2, season_id: 2, episode_id: 5, title: 'Server Space', air_date: new Date('2015-05-10') },
    { series_id: 2, season_id: 2, episode_id: 6, title: 'Homicide', air_date: new Date('2015-05-17') },
    { series_id: 2, season_id: 2, episode_id: 7, title: 'Adult Content', air_date: new Date('2015-05-24') },
    { series_id: 2, season_id: 2, episode_id: 8, title: 'White Hat/Black Hat', air_date: new Date('2015-05-31') },
    { series_id: 2, season_id: 2, episode_id: 9, title: 'Binding Arbitration', air_date: new Date('2015-06-07') },
    { series_id: 2, season_id: 2, episode_id: 1, title: 'Two Days of the Condor', air_date: new Date('2015-06-14') },
    { series_id: 2, season_id: 3, episode_id: 1, title: 'Founder Friendly', air_date: new Date('2016-04-24') },
    { series_id: 2, season_id: 3, episode_id: 2, title: 'Two in the Box', air_date: new Date('2016-05-01') },
    { series_id: 2, season_id: 3, episode_id: 3, title: 'Meinertzhagen\'s Haversack', air_date: new Date('2016-05-08') },
    { series_id: 2, season_id: 3, episode_id: 4, title: 'Maleant Data Systems Solutions', air_date: new Date('2016-05-15') },
    { series_id: 2, season_id: 3, episode_id: 5, title: 'The Empty Chair', air_date: new Date('2016-05-22') },
    { series_id: 2, season_id: 3, episode_id: 6, title: 'Bachmanity Insanity', air_date: new Date('2016-05-29') },
    { series_id: 2, season_id: 3, episode_id: 7, title: 'To Build a Better Beta', air_date: new Date('2016-06-05') },
    { series_id: 2, season_id: 3, episode_id: 8, title: 'Bachman\'s Earnings Over-Ride', air_date: new Date('2016-06-12') },
    { series_id: 2, season_id: 3, episode_id: 9, title: 'Daily Active Users', air_date: new Date('2016-06-19') },
    { series_id: 2, season_id: 3, episode_id: 1, title: 'The Uptick', air_date: new Date('2016-06-26') },
    { series_id: 2, season_id: 4, episode_id: 1, title: 'Success Failure', air_date: new Date('2017-04-23') },
    { series_id: 2, season_id: 4, episode_id: 2, title: 'Terms of Service', air_date: new Date('2017-04-30') },
    { series_id: 2, season_id: 4, episode_id: 3, title: 'Intellectual Property', air_date: new Date('2017-05-07') },
    { series_id: 2, season_id: 4, episode_id: 4, title: 'Teambuilding Exercise', air_date: new Date('2017-05-14') },
    { series_id: 2, season_id: 4, episode_id: 5, title: 'The Blood Boy', air_date: new Date('2017-05-21') },
    { series_id: 2, season_id: 4, episode_id: 6, title: 'Customer Service', air_date: new Date('2017-05-28') },
    { series_id: 2, season_id: 4, episode_id: 7, title: 'The Patent Troll', air_date: new Date('2017-06-04') },
    { series_id: 2, season_id: 4, episode_id: 8, title: 'The Keenan Vortex', air_date: new Date('2017-06-11') },
    { series_id: 2, season_id: 4, episode_id: 9, title: 'Hooli-Con', air_date: new Date('2017-06-18') },
    { series_id: 2, season_id: 4, episode_id: 1, title: 'Server Error', air_date: new Date('2017-06-25') },
    { series_id: 2, season_id: 5, episode_id: 1, title: 'Grow Fast or Die Slow', air_date: new Date('2018-03-25') },
    { series_id: 2, season_id: 5, episode_id: 2, title: 'Reorientation', air_date: new Date('2018-04-01') },
    { series_id: 2, season_id: 5, episode_id: 3, title: 'Chief Operating Officer', air_date: new Date('2018-04-08') },
    { series_id: 2, season_id: 5, episode_id: 4, title: 'Tech Evangelist', air_date: new Date('2018-04-15') },
    { series_id: 2, season_id: 5, episode_id: 5, title: 'Facial Recognition', air_date: new Date('2018-04-22') },
    { series_id: 2, season_id: 5, episode_id: 6, title: 'Artificial Emotional Intelligence', air_date: new Date('2018-04-29') },
    { series_id: 2, season_id: 5, episode_id: 7, title: 'Initial Coin Offering', air_date: new Date('2018-05-06') },
    { series_id: 2, season_id: 5, episode_id: 8, title: 'Fifty-One Percent', air_date: new Date('2018-05-13') },
  ]
}

/* ------------------------------------------------------------------ */
/* insert                                                             */
/* ------------------------------------------------------------------ */

export async function insertDefaultDataSet(driver: Driver): Promise<void> {
  const sql = query(driver)

  await sql`REPLACE INTO ${sql.identifier(SERIES_TABLE)}
    SELECT * FROM AS_TABLE(${getSeriesData()});

    REPLACE INTO ${sql.identifier(SEASONS_TABLE)}
    SELECT * FROM AS_TABLE(${getSeasonsData()});

    REPLACE INTO ${sql.identifier(EPISODES_TABLE)}
    SELECT * FROM AS_TABLE(${getEpisodesData()});
  `
}
