import { Sequelize } from 'sequelize-typescript';
import { get, orderBy } from "lodash";

import {
    StatusEnum,
    createSequelizeConnection,
    getTableName,
    loadEnvironmentVariables,
} from './common';

class FieldDiffOutput {
    fieldName!: string;
    count!: number;
}

async function main(): Promise<void> {
    loadEnvironmentVariables();
    await computeStatistics();
}

async function computeStatistics(): Promise<void> {
    const localDBConnection = await createSequelizeConnection();

    const fieldDifferenceCounts: Map<string, number> = new Map<string, number>();
    const missingFieldsCounts: Map<string, number> = new Map<string, number>();

    let shouldContinue = true;
    while (shouldContinue) {
        shouldContinue = await countForOnePolicy(localDBConnection, fieldDifferenceCounts, missingFieldsCounts);
    }

    console.log('Field Differences');
    console.log(JSON.stringify(getOrderedCounts(fieldDifferenceCounts), null, 2));

    console.log('Missing Fields on Policies');
    console.log(JSON.stringify(getOrderedCounts(missingFieldsCounts), null, 2));
}

function getOrderedCounts(map: Map<string, number>): FieldDiffOutput[] {
    const ret: FieldDiffOutput[] = [];

    map.forEach((value, key) => {
        ret.push({ fieldName: key, count: value });
    });

    return orderBy(ret, ['count', 'fieldName'], ['desc', 'asc']);
}

async function countForOnePolicy (
    localDBConnection: Sequelize,
    fieldDifferenceCounts: Map<string, number>,
    missingFieldsCounts: Map<string, number>,
): Promise<boolean> {

    const transaction = await localDBConnection.transaction();
    const tableName = getTableName();

    const getDifferencesQuery = `select `
        + `id, `
        + `field_differences, `
        + `missing_policy_fields `
        + `from ${tableName} `
        + `where status = '${StatusEnum.Processed}' `
        + `limit 1 for update skip locked; `;

    const [ results ] = await localDBConnection.query(getDifferencesQuery, { transaction });

    // do not continue as all rows have been handled
    if (results.length === 0) {
        await transaction.commit();
        return false;

    } else {
        const responseRow = results[0] as Record<string, unknown>;

        const rowId = get(responseRow, 'id') as string;
        const fieldDifferences = get(responseRow, 'field_differences') as Array<string>;
        const missingPolicyFields = get(responseRow, 'missing_policy_fields') as Array<string>;

        fieldDifferences.forEach((field) => {
            const currentCount = fieldDifferenceCounts.get(field) ?? 0;
            fieldDifferenceCounts.set(field, currentCount + 1)
        });

        missingPolicyFields.forEach((field) => {
            const currentCount = missingFieldsCounts.get(field) ?? 0;
            missingFieldsCounts.set(field, currentCount + 1)
        });

        const updateSuccessQuery = `update ${tableName} `
            + `set status ='${StatusEnum.Complete}' `
            + `where id = '${rowId}';`;
        await localDBConnection.query(updateSuccessQuery, { transaction });
        await transaction.commit();

        // script should continue by handling next row
        return true;
    }
}

if (require.main === module) {
    main()
        .then(() => {
            console.log('Successfully finished comparing property data!');
            process.exit(0);
        })
        .catch(
            (err) => {
                console.log(err);
                process.exit(1);
            },
        );
}
