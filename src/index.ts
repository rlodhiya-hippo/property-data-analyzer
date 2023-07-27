import {
    API_CLIENT as PROPERTY_SERVICE_API_CLIENT,
    API as PropertyServiceAPI,
    ApiModule as PropertyServiceApiModule,
} from '@hippo/property-service-api-v1';
import {
    API_CLIENT as POLICY_SERVICE_API_CLIENT,
    API as PolicyServiceAPI,
    ApiModule as PolicyServiceApiModule,
} from '@hippo/policy-service-api-v1';
import { NestFactory } from '@nestjs/core';
import { INestApplicationContext, Module } from '@nestjs/common';
import { Sequelize } from 'sequelize-typescript';
import { get } from 'lodash';

@Module({
    imports: [
        PropertyServiceApiModule,
        PolicyServiceApiModule,
    ],
})
class PropertyDataAnalyzerModule {
}

const enum FetchStatus {
    Pending = 'pending',
    Complete = 'complete',
    Failed = 'failed',
}

async function createPropertyDataAnalyzerApp(): Promise<INestApplicationContext> {
    const app = await NestFactory.createApplicationContext(PropertyDataAnalyzerModule);
    return app;
}

async function getPropertyServiceApiClient(app: INestApplicationContext): Promise<PropertyServiceAPI> {
    return app.get<PropertyServiceAPI>(PROPERTY_SERVICE_API_CLIENT);
}

async function getPolicyServiceApiClient(app: INestApplicationContext): Promise<PolicyServiceAPI> {
    return app.get<PolicyServiceAPI>(POLICY_SERVICE_API_CLIENT);
}

async function main(): Promise<void> {
    const app = await createPropertyDataAnalyzerApp();
    const propertyServiceAPIClient = await getPropertyServiceApiClient(app);
    const policyServiceAPIClient = await getPolicyServiceApiClient(app);

    const localDBConnection = new Sequelize({
        dialect: 'postgres',
        host: process.env.SEQUELIZE_HOST,
        port: Number(process.env.SEQUELIZE_PORT),
        username: process.env.SEQUELIZE_USERNAME,
        password: process.env.SEQUELIZE_PASSWORD,
        database: process.env.SEQUELIZE_DATABASE,
    });

    try {
        await Promise.all([
            localDBConnection.authenticate(),
        ]);
    } catch (err) {
        console.log(JSON.stringify(err, null, 2));
        throw err;
    }

    let shouldContinue: boolean = true;

    while (shouldContinue) {
        shouldContinue = await handleOneRow(localDBConnection, propertyServiceAPIClient, policyServiceAPIClient);
    }
}

async function handleOneRow(
    localDBConnection: Sequelize,
    propertyServiceAPIClient: PropertyServiceAPI,
    policyServiceAPIClient: PolicyServiceAPI,
): Promise<boolean> {
    const transaction = await localDBConnection.transaction();

    const tableName = process.env.SEQUELIZE_TABLENAME as string;

    const retrieveRowQuery = `select `
        + `id, `
        + `policy_id, `
        + `policy_number, `
        + `policy_uuid, `
        + `application_uuid, `
        + `policy_bound_date, `
        + `property_id `
        + `from ${tableName} `
        + `where fetched = '${FetchStatus.Pending}' `
        + `limit 1 for update skip locked `;

    const [ results] = await localDBConnection.query(retrieveRowQuery, { transaction });

    // do not continue as all rows have been handled
    if (results.length === 0) {
        await transaction.commit();
        return false;
    }

    if (results.length > 0) {
        const responseRow = results[0] as Record<string, unknown>;
        console.log(JSON.stringify(responseRow, null, 2));

        const rowId = get(responseRow, 'id') as string;
        const policyId = get(responseRow, 'policy_uuid') as string;
        const propertyId = get(responseRow, 'property_id') as string;

        console.log(`Row ID: ${rowId}`);

        try {
            const vendorPropertyData = await propertyServiceAPIClient.unary.validatedPropertyRetrieveVendorData({ id: propertyId });
            const propertyData = await propertyServiceAPIClient.unary.validatedPropertyRetrieve({ id: propertyId, revision: vendorPropertyData.revision });
            const policyData = await policyServiceAPIClient.unary.insurancePolicyRetrieve({ id: policyId });

            console.log('Vendor Property Data');
            console.log(JSON.stringify(vendorPropertyData, null, 2));

            console.log('Property Data');
            console.log(JSON.stringify(propertyData, null, 2));

            console.log('Policy Data');
            console.log(JSON.stringify(policyData, null, 2));

            /* TODO: Persist JSON to file or local DB ? */
            /* TODO: Perform Comparison ? */

            const updateSuccessQuery = `update ${tableName} `
                + `set fetched ='${FetchStatus.Complete}' `
                + `where id='${rowId}'`;
            await localDBConnection.query(updateSuccessQuery, { transaction });
            await transaction.commit();

        } catch (error) {
            console.log(JSON.stringify(error, null, 2));
            const updateFailureQuery = `update ${tableName} `
                + `set fetched = '${FetchStatus.Failed}' `
                + `where id='${rowId}'`;

            await localDBConnection.query(updateFailureQuery, { transaction });
            await transaction.commit();
        }
    }

    // script should continue by handling next row
    return true;
}

if (require.main === module) {
    main()
        .then(() => {
            console.log('Successfully finished fetching property + policy data!');
            process.exit(1);
        })
        .catch(
            (err) => {
                console.log(err);
                process.exit(1);
            },
        );
}
