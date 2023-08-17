import { Sequelize } from 'sequelize-typescript';
import * as yargs from 'yargs';
import * as dotenv from 'dotenv';

class PropertyDataAnalyzerArgs {
    numberOfThreads?: number;
}

export enum StatusEnum {
    Pending = 'pending',
    Fetched = 'fetched',
    Processed = 'processed',
    Complete = 'complete',
    Failed = 'failed',
}

export function isNullOrUndefined<T>(value: T): boolean {
    if (value === undefined || value === null) {
        return true;
    }
    return false;
}

export function loadEnvironmentVariables(): void {
    const output = dotenv.config({ path: './.env' });
    console.log(output);
}

function parsePropertyDataAnalyzerArgs(): PropertyDataAnalyzerArgs {
    return yargs.usage(
        'Usage: analyze-ppc-reports <options>',
    ).options({
        numberOfThreads: {
            demandOption: false,
            type: 'number',
        },
    }).argv as PropertyDataAnalyzerArgs;
}

export function getNumberOfThreads(): number {
    const args = parsePropertyDataAnalyzerArgs();
    let numberOfThreads = Math.abs(args.numberOfThreads ?? 1);
    if (numberOfThreads > 50) {
        console.log(`Number of threads is capped at 50, ${numberOfThreads} is not supported!`);
        numberOfThreads = 50;
    }
    return numberOfThreads;
}

export async function createSequelizeConnection(): Promise<Sequelize> {
    const localDBConnection = new Sequelize({
        dialect: 'postgres',
        host: process.env.SEQUELIZE_HOST,
        port: Number(process.env.SEQUELIZE_PORT),
        username: process.env.SEQUELIZE_USERNAME,
        password: process.env.SEQUELIZE_PASSWORD,
        database: process.env.SEQUELIZE_DATABASE,
    });

    try {
        await localDBConnection.authenticate();
    } catch (err) {
        console.log(JSON.stringify(err, null, 2));
        throw err;
    }

    return localDBConnection;
}

export function getTableName(): string {
    const tableName = process.env.SEQUELIZE_TABLENAME as string;
    return tableName;
}
