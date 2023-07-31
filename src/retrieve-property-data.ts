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
import { fill, get } from 'lodash';

import {
    StatusEnum,
    createSequelizeConnection,
    getNumberOfThreads,
    getTableName, loadEnvironmentVariables,
} from './common';

@Module({
    imports: [
        PropertyServiceApiModule,
        PolicyServiceApiModule,
    ],
})
class PropertyDataAnalyzerModule {
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
    loadEnvironmentVariables();
    const app = await createPropertyDataAnalyzerApp();
    const propertyServiceAPIClient = await getPropertyServiceApiClient(app);
    const policyServiceAPIClient = await getPolicyServiceApiClient(app);

    const numberOfThreads = getNumberOfThreads();
    const threadArray = fill(new Array<number>(numberOfThreads), 0);
    await Promise.all(threadArray.map(() => startThread(propertyServiceAPIClient, policyServiceAPIClient)));
}

async function startThread(
    propertyServiceAPIClient: PropertyServiceAPI,
    policyServiceAPIClient: PolicyServiceAPI,
): Promise<void> {
    const localDBConnection = await createSequelizeConnection();

    let shouldContinue: boolean = true;
    while (shouldContinue) {
        shouldContinue = await retrievePropertyDataForOnePolicy(localDBConnection, propertyServiceAPIClient, policyServiceAPIClient);
    }
}

async function retrievePropertyDataForOnePolicy(
    localDBConnection: Sequelize,
    propertyServiceAPIClient: PropertyServiceAPI,
    policyServiceAPIClient: PolicyServiceAPI,
): Promise<boolean> {
    const transaction = await localDBConnection.transaction();

    const tableName = getTableName();

    const retrieveRowQuery = `select `
        + `id, `
        + `policy_uuid, `
        + `property_id `
        + `from ${tableName} `
        + `where status = '${StatusEnum.Pending}' `
        + `limit 1 for update skip locked; `;

    const [ results] = await localDBConnection.query(retrieveRowQuery, { transaction });

    // do not continue as all rows have been handled
    if (results.length === 0) {
        await transaction.commit();
        return false;

    } else {
        const responseRow = results[0] as Record<string, unknown>;

        const rowId = get(responseRow, 'id') as string;
        const policyId = get(responseRow, 'policy_uuid') as string;
        const propertyId = get(responseRow, 'property_id') as string;

        console.log(`Row ID: ${rowId}`);

        try {
            const vendorPropertyData = await propertyServiceAPIClient.unary.validatedPropertyRetrieveVendorData({ id: propertyId });
            const policyData = await policyServiceAPIClient.unary.insurancePolicyRetrieve({ id: policyId });

            /* escape ' char for writing string - can try to use sequelize bind / replacements to handle string char escape */
            const vendorPropertyDataJson = JSON.stringify(vendorPropertyData);
            const policyDataJson = JSON.stringify(policyData);

            const updateSuccessQuery = `update ${tableName} `
                + `set status ='${StatusEnum.Fetched}', `
                + ` policy_data = to_jsonb( :policyJson ::json), `
                + ` vendor_property_data = to_jsonb( :vendorPropertyJson ::json) `
                + `where id = '${rowId}';`;
            await localDBConnection.query(updateSuccessQuery, {
                transaction,
                replacements: {
                    policyJson: policyDataJson,
                    vendorPropertyJson: vendorPropertyDataJson,
                },
            });
            await transaction.commit();

        } catch (error) {
            console.log(JSON.stringify(error, null, 2));
            const updateFailureQuery = `update ${tableName} `
                + `set status = '${StatusEnum.Failed}' `
                + `where id = '${rowId}';`;

            await localDBConnection.query(updateFailureQuery, { transaction });
            await transaction.commit();
        }

        // script should continue by handling next row
        return true;
    }
}

if (require.main === module) {
    main()
        .then(() => {
            console.log('Successfully finished fetching property + policy data!');
            process.exit(0);
        })
        .catch(
            (err) => {
                console.log(err);
                process.exit(1);
            },
        );
}
