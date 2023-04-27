import {
    Driver, ExecuteQuerySettings, RetryParameters, Session, withRetries, Ydb
} from 'ydb-sdk';

export type IQueryParams = {
    readonly [k: string]: Ydb.ITypedValue;
};

const querySettings = new ExecuteQuerySettings()
    .withKeepInCache(true);
const retrySettings = new RetryParameters({
    maxRetries: 2,
});

export const executeYdbQuery = async (dbSess: Session, queryStr: string, queryParams?: IQueryParams) => {
    return withRetries(async () => {
        const preparedQuery = await dbSess.prepareQuery(queryStr);
        return dbSess.executeQuery(preparedQuery, queryParams, undefined, querySettings);
    }, retrySettings);
};

export const executeYdbQueryWithSession = async(driver: Driver, queryStr: string, queryParams?: IQueryParams) => {
    return await driver.tableClient.withSession(async (session) => {
        return executeYdbQuery(session, queryStr, queryParams)
    });
}
