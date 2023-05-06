import fs from 'fs';
import path from 'path';

import {
  AnonymousAuthService,
  Column,
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  declareType,
  Driver,
  getLogger,
  IDriverSettings,
  TableDescription,
  TableIndex,
  TypedData,
  Types,
} from 'ydb-sdk';

export const SYNTAX_V1 = '--!syntax_v1';

export const SERIES_TABLE = 'series';
export const SEASONS_TABLE = 'seasons';
export const EPISODES_TABLE = 'episodes';

const YDB_ENDPOINT = 'grpcs://localhost:2135';
const YDB_DB = '/local';

export interface Series {
  series_id: number;
  title: string | null;
  series_info: string | null;
  release_date: Date | null;
  is_closed: boolean | null;
}

export interface Seasons {
  series_id: number;
  season_id: number;
  title: string | null;
  first_aired: Date | null;
  last_aired: Date | null;
}

export interface Episodes {
  series_id: number;
  season_id: number;
  episode_id: number;
  title: string | null;
  air_date: Date | null;
}

export interface Database {
  series: Series;
  seasons: Seasons;
  episodes: Episodes;
}

const certFile =
  process.env.YDB_SSL_ROOT_CERTIFICATES_FILE ||
  path.join(process.cwd(), 'ydb_certs/ca.pem');
if (!fs.existsSync(certFile)) {
  throw new Error(
    `Certificate file ${certFile} doesn't exist! Please use YDB_SSL_ROOT_CERTIFICATES_FILE env variable or run Docker container https://cloud.yandex.ru/docs/ydb/getting_started/ydb_docker inside working directory`
  );
}

const sslCredentials = { rootCertificates: fs.readFileSync(certFile) };
const authService = new AnonymousAuthService();
export const driverSettings: IDriverSettings = {
  endpoint: YDB_ENDPOINT,
  database: YDB_DB,
  authService: authService,
  sslCredentials: sslCredentials,
  logger: getLogger()
}

export async function initTest(driver: Driver) {
  const timeout = 10000;
    if (!await driver.ready(timeout)) {
        throw new Error(`Driver has not become ready in ${timeout}ms!`);
    }
  await dropTables(driver);
  return await createTables(driver);
}

export async function destroyTest(driver: Driver): Promise<void> {
  await dropTables(driver);
  return await driver.destroy();
}

export async function clearDatabase(driver: Driver): Promise<void> {
  return await driver.tableClient.withSession(async (session) => {
    await session.executeQuery(`DELETE FROM ${SERIES_TABLE}`);
    await session.executeQuery(`DELETE FROM ${SEASONS_TABLE}`);
    await session.executeQuery(`DELETE FROM ${EPISODES_TABLE}`);
  });
}

export async function createTables(driver: Driver): Promise<void> {
  return await driver.tableClient.withSession(async (session) => {
    await session.createTable(
      SERIES_TABLE,
      new TableDescription()
        .withColumn(new Column('series_id', Types.optional(Types.UINT32)))
        .withColumn(new Column('title', Types.optional(Types.UTF8)))
        .withColumn(new Column('series_info', Types.optional(Types.UTF8)))
        .withColumn(new Column('release_date', Types.optional(Types.DATE)))
        .withColumn(new Column('is_closed', Types.optional(Types.BOOL)))
        .withPrimaryKey('series_id')
    );

    await session.createTable(
      SEASONS_TABLE,
      new TableDescription()
        .withColumn(new Column('series_id', Types.optional(Types.UINT32)))
        .withColumn(new Column('season_id', Types.optional(Types.UINT32)))
        .withColumn(new Column('title', Types.optional(Types.TEXT)))
        .withColumn(new Column('first_aired', Types.optional(Types.DATE)))
        .withColumn(new Column('last_aired', Types.optional(Types.DATE)))
        .withPrimaryKeys('series_id', 'season_id')
    );

    const episodesIndex = new TableIndex('episodes_index')
      .withIndexColumns('title')
      .withDataColumns('air_date')
      .withGlobalAsync(true);

    await session.createTable(
      EPISODES_TABLE,
      new TableDescription()
        .withColumn(new Column('series_id', Types.optional(Types.UINT32)))
        .withColumn(new Column('season_id', Types.optional(Types.UINT32)))
        .withColumn(new Column('episode_id', Types.optional(Types.UINT64)))
        .withColumn(new Column('title', Types.optional(Types.UTF8)))
        .withColumn(new Column('air_date', Types.optional(Types.DATE)))
        .withPrimaryKeys('series_id', 'season_id', 'episode_id')
        .withIndex(episodesIndex)
    );
  });
}

export async function dropTables(driver: Driver): Promise<void> {
  return await driver.tableClient.withSession(async (session) => {
    await session.dropTable(SERIES_TABLE);
    await session.dropTable(EPISODES_TABLE);
    await session.dropTable(SEASONS_TABLE);
  });
}

export class YdbSeries extends TypedData implements Series  {
    @declareType(Types.UINT32)
    public series_id: number;

    @declareType(Types.UTF8)
    public title: string | null;

    @declareType(Types.DATE)
    public release_date: Date | null;

    @declareType(Types.UTF8)
    public series_info: string | null;

    @declareType(Types.BOOL)
    public is_closed: boolean | null;

    static create(series_id: number, title: string, release_date: Date, series_info: string, is_closed: boolean): YdbSeries {
        return new this({series_id, title, release_date, series_info, is_closed});
    }

    constructor(data: Series) {
        super(data);
        this.series_id = data.series_id;
        this.title = data.title;
        this.release_date = data.release_date;
        this.series_info = data.series_info;
        this.is_closed = data.is_closed
    }
}

export class YdbEpisode extends TypedData implements Episodes {
    @declareType(Types.UINT32)
    public series_id: number;

    @declareType(Types.UINT32)
    public season_id: number;

    @declareType(Types.UINT64)
    public episode_id: number;

    @declareType(Types.UTF8)
    public title: string | null;

    @declareType(Types.DATE)
    public air_date: Date | null;

    static create(series_id: number, season_id: number, episode_id: number, title: string, air_date: Date): YdbEpisode {
        return new this({series_id, season_id, episode_id, title, air_date});
    }

    constructor(data: Episodes) {
        super(data);
        this.series_id = data.series_id;
        this.season_id = data.season_id;
        this.episode_id = data.episode_id;
        this.title = data.title;
        this.air_date = data.air_date;
    }
}

export class YdbSeason extends TypedData implements Seasons {
    @declareType(Types.UINT32)
    public series_id: number;

    @declareType(Types.UINT32)
    public season_id: number;

    @declareType(Types.UTF8)
    public title: string | null;

    @declareType(Types.DATE)
    public first_aired: Date | null;

    @declareType(Types.DATE)
    public last_aired: Date | null;

    static create(series_id: number, season_id: number, title: string, first_aired: Date, last_aired: Date): YdbSeason {
        return new this({series_id, season_id, title, first_aired, last_aired});
    }

    constructor(data: Seasons) {
        super(data);
        this.series_id = data.series_id;
        this.season_id = data.season_id;
        this.title = data.title;
        this.first_aired = data.first_aired;
        this.last_aired = data.last_aired;
    }
}

export function getSeriesData() {
  return YdbSeries.asTypedCollection([
      YdbSeries.create(
        1, "IT Crowd", new Date("2006-02-03"),
          "The IT Crowd is a British sitcom produced by Channel 4, written by Graham Linehan, produced by " +
          "Ash Atalla and starring Chris O'Dowd, Richard Ayoade, Katherine Parkinson, and Matt Berry.",
          true),
      YdbSeries.create(2, "Silicon Valley",  new Date("2014-04-06"),
          "Silicon Valley is an American comedy television series created by Mike Judge, John Altschuler and " +
          "Dave Krinsky. The series focuses on five young men who founded a startup company in Silicon Valley.",
          true)
  ]);
}

export function getSeasonsData() {
  return YdbSeason.asTypedCollection([
      YdbSeason.create(1, 1, "Season 1", new Date("2006-02-03"), new Date("2006-03-03")),
      YdbSeason.create(1, 2, "Season 2", new Date("2007-08-24"), new Date("2007-09-28")),
      YdbSeason.create(1, 3, "Season 3", new Date("2008-11-21"), new Date("2008-12-26")),
      YdbSeason.create(1, 4, "Season 4", new Date("2010-06-25"), new Date("2010-07-30")),
      YdbSeason.create(2, 1, "Season 1", new Date("2014-04-06"), new Date("2014-06-01")),
      YdbSeason.create(2, 2, "Season 2", new Date("2015-04-12"), new Date("2015-06-14")),
      YdbSeason.create(2, 3, "Season 3", new Date("2016-04-24"), new Date("2016-06-26")),
      YdbSeason.create(2, 4, "Season 4", new Date("2017-04-23"), new Date("2017-06-25")),
      YdbSeason.create(2, 5, "Season 5", new Date("2018-03-25"), new Date("2018-05-13"))
  ]);
}

export function getEpisodesData() {
  return YdbEpisode.asTypedCollection([
      YdbEpisode.create(1, 1, 1, "Yesterday's Jam", new Date("2006-02-03")),
      YdbEpisode.create(1, 1, 2, "Calamity Jen", new Date("2006-02-03")),
      YdbEpisode.create(1, 1, 3, "Fifty-Fifty", new Date("2006-02-10")),
      YdbEpisode.create(1, 1, 4, "The Red Door", new Date("2006-02-17")),
      YdbEpisode.create(1, 1, 5, "The Haunting of Bill Crouse", new Date("2006-02-24")),
      YdbEpisode.create(1, 1, 6, "Aunt Irma Visits", new Date("2006-03-03")),
      YdbEpisode.create(1, 2, 1, "The Work Outing", new Date("2006-08-24")),
      YdbEpisode.create(1, 2, 2, "Return of the Golden Child", new Date("2007-08-31")),
      YdbEpisode.create(1, 2, 3, "Moss and the German", new Date("2007-09-07")),
      YdbEpisode.create(1, 2, 4, "The Dinner Party", new Date("2007-09-14")),
      YdbEpisode.create(1, 2, 5, "Smoke and Mirrors", new Date("2007-09-21")),
      YdbEpisode.create(1, 2, 6, "Men Without Women", new Date("2007-09-28")),
      YdbEpisode.create(1, 3, 1, "From Hell", new Date("2008-11-21")),
      YdbEpisode.create(1, 3, 2, "Are We Not Men?", new Date("2008-11-28")),
      YdbEpisode.create(1, 3, 3, "Tramps Like Us", new Date("2008-12-05")),
      YdbEpisode.create(1, 3, 4, "The Speech", new Date("2008-12-12")),
      YdbEpisode.create(1, 3, 5, "Friendface", new Date("2008-12-19")),
      YdbEpisode.create(1, 3, 6, "Calendar Geeks", new Date("2008-12-26")),
      YdbEpisode.create(1, 4, 1, "Jen The Fredo", new Date("2010-06-25")),
      YdbEpisode.create(1, 4, 2, "The Final Countdown", new Date("2010-07-02")),
      YdbEpisode.create(1, 4, 3, "Something Happened", new Date("2010-07-09")),
      YdbEpisode.create(1, 4, 4, "Italian For Beginners", new Date("2010-07-16")),
      YdbEpisode.create(1, 4, 5, "Bad Boys", new Date("2010-07-23")),
      YdbEpisode.create(1, 4, 6, "Reynholm vs Reynholm", new Date("2010-07-30")),
      YdbEpisode.create(2, 1, 1, "Minimum Viable Product", new Date("2014-04-06")),
      YdbEpisode.create(2, 1, 2, "The Cap Table", new Date("2014-04-13")),
      YdbEpisode.create(2, 1, 3, "Articles of Incorporation", new Date("2014-04-20")),
      YdbEpisode.create(2, 1, 4, "Fiduciary Duties", new Date("2014-04-27")),
      YdbEpisode.create(2, 1, 5, "Signaling Risk", new Date("2014-05-04")),
      YdbEpisode.create(2, 1, 6, "Third Party Insourcing", new Date("2014-05-11")),
      YdbEpisode.create(2, 1, 7, "Proof of Concept", new Date("2014-05-18")),
      YdbEpisode.create(2, 1, 8, "Optimal Tip-to-Tip Efficiency", new Date("2014-06-01")),
      YdbEpisode.create(2, 2, 1, "Sand Hill Shuffle", new Date("2015-04-12")),
      YdbEpisode.create(2, 2, 2, "Runaway Devaluation", new Date("2015-04-19")),
      YdbEpisode.create(2, 2, 3, "Bad Money", new Date("2015-04-26")),
      YdbEpisode.create(2, 2, 4, "The Lady", new Date("2015-05-03")),
      YdbEpisode.create(2, 2, 5, "Server Space", new Date("2015-05-10")),
      YdbEpisode.create(2, 2, 6, "Homicide", new Date("2015-05-17")),
      YdbEpisode.create(2, 2, 7, "Adult Content", new Date("2015-05-24")),
      YdbEpisode.create(2, 2, 8, "White Hat/Black Hat", new Date("2015-05-31")),
      YdbEpisode.create(2, 2, 9, "Binding Arbitration", new Date("2015-06-07")),
      YdbEpisode.create(2, 2, 1, "Two Days of the Condor", new Date("2015-06-14")),
      YdbEpisode.create(2, 3, 1, "Founder Friendly", new Date("2016-04-24")),
      YdbEpisode.create(2, 3, 2, "Two in the Box", new Date("2016-05-01")),
      YdbEpisode.create(2, 3, 3, "Meinertzhagen's Haversack", new Date("2016-05-08")),
      YdbEpisode.create(2, 3, 4, "Maleant Data Systems Solutions", new Date("2016-05-15")),
      YdbEpisode.create(2, 3, 5, "The Empty Chair", new Date("2016-05-22")),
      YdbEpisode.create(2, 3, 6, "Bachmanity Insanity", new Date("2016-05-29")),
      YdbEpisode.create(2, 3, 7, "To Build a Better Beta", new Date("2016-06-05")),
      YdbEpisode.create(2, 3, 8, "Bachman's Earnings Over-Ride", new Date("2016-06-12")),
      YdbEpisode.create(2, 3, 9, "Daily Active Users", new Date("2016-06-19")),
      YdbEpisode.create(2, 3, 1, "The Uptick", new Date("2016-06-26")),
      YdbEpisode.create(2, 4, 1, "Success Failure", new Date("2017-04-23")),
      YdbEpisode.create(2, 4, 2, "Terms of Service", new Date("2017-04-30")),
      YdbEpisode.create(2, 4, 3, "Intellectual Property", new Date("2017-05-07")),
      YdbEpisode.create(2, 4, 4, "Teambuilding Exercise", new Date("2017-05-14")),
      YdbEpisode.create(2, 4, 5, "The Blood Boy", new Date("2017-05-21")),
      YdbEpisode.create(2, 4, 6, "Customer Service", new Date("2017-05-28")),
      YdbEpisode.create(2, 4, 7, "The Patent Troll", new Date("2017-06-04")),
      YdbEpisode.create(2, 4, 8, "The Keenan Vortex", new Date("2017-06-11")),
      YdbEpisode.create(2, 4, 9, "Hooli-Con", new Date("2017-06-18")),
      YdbEpisode.create(2, 4, 1, "Server Error", new Date("2017-06-25")),
      YdbEpisode.create(2, 5, 1, "Grow Fast or Die Slow", new Date("2018-03-25")),
      YdbEpisode.create(2, 5, 2, "Reorientation", new Date("2018-04-01")),
      YdbEpisode.create(2, 5, 3, "Chief Operating Officer", new Date("2018-04-08")),
      YdbEpisode.create(2, 5, 4, "Tech Evangelist", new Date("2018-04-15")),
      YdbEpisode.create(2, 5, 5, "Facial Recognition", new Date("2018-04-22")),
      YdbEpisode.create(2, 5, 6, "Artificial Emotional Intelligence", new Date("2018-04-29")),
      YdbEpisode.create(2, 5, 7, "Initial Coin Offering", new Date("2018-05-06")),
      YdbEpisode.create(2, 5, 8, "Fifty-One Percent", new Date("2018-05-13")),
  ]);
}

export async function insertDefaultDataSet(driver: Driver) {
  const query = `
    ${SYNTAX_V1}
    DECLARE $seriesData AS List<Struct<
        series_id: Uint32,
        title: Utf8,
        series_info: Utf8,
        release_date: Date,
        is_closed: Bool>>;
    DECLARE $seasonsData AS List<Struct<
        series_id: Uint32,
        season_id: Uint32,
        title: Utf8,
        first_aired: Date,
        last_aired: Date>>;
    DECLARE $episodesData AS List<Struct<
        series_id: Uint32,
        season_id: Uint32,
        episode_id: Uint64,
        title: Utf8,
        air_date: Date>>;

    REPLACE INTO ${SERIES_TABLE}
    SELECT
        series_id,
        title,
        series_info,
        release_date,
        is_closed
    FROM AS_TABLE($seriesData);

    REPLACE INTO ${SEASONS_TABLE}
    SELECT
        series_id,
        season_id,
        title,
        first_aired,
        last_aired
    FROM AS_TABLE($seasonsData);

    REPLACE INTO ${EPISODES_TABLE}
    SELECT
        series_id,
        season_id,
        episode_id,
        title,
        air_date
    FROM AS_TABLE($episodesData);`;
    return driver.tableClient.withSession(async (session) => {
      const preparedQuery = await session.prepareQuery(query);
      await session.executeQuery(preparedQuery, {
        '$seriesData': getSeriesData(),
        '$seasonsData': getSeasonsData(),
        '$episodesData': getEpisodesData()
      });
    });

};
