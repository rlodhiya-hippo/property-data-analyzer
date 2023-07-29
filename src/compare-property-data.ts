import { Sequelize } from 'sequelize-typescript';
import { fill, get, isEqual, isEmpty } from 'lodash';
import { JSONPath } from 'jsonpath-plus';

import {
    StatusEnum,
    createSequelizeConnection,
    isNullOrUndefined,
    getNumberOfThreads,
    getTableName,
    loadEnvironmentVariables,
} from './common';

class FieldPath {
    fieldName!: string;
    propertyDataPath!: string;
    policyDataPath!: string;
}

class CompareResponse {
    mismatch!: Set<string>;
    missingOnPolicyData!: Set<string>;
}

const compareFieldPaths: FieldPath[] = [

    // check for sanity
    { fieldName: 'propertyRevision', propertyDataPath: '$.revision', policyDataPath: '$.property.revision' },

    // Verisk Valuation Roof fields
    { fieldName: 'yearRoofBuilt', propertyDataPath: '$.roof.yearBuilt', policyDataPath: '$.property.roof.yearBuilt' },
    { fieldName: 'roofMaterial', propertyDataPath: '$.roof.material', policyDataPath: '$.property.roof.material' }, // reconciled w/ Cape
    { fieldName: 'roofType', propertyDataPath: '$.roof.type', policyDataPath: '$.property.roof.type' }, // reconciled w/ Cape

    // Cape Roof fields
    { fieldName: 'roofConditionRating', propertyDataPath: '$.roof.conditionRating', policyDataPath: '$.property.roof.conditionRating' },
    { fieldName: 'roofTreeOverhang', propertyDataPath: '$.roof.treeOverhang', policyDataPath: '$.property.roof.treeOverhang' },
    { fieldName: 'roofSquareFootage', propertyDataPath: '$.roof.squareFootage', policyDataPath: '$.property.roof.squareFootage' },
    { fieldName: 'roofFacetCount', propertyDataPath: '$.roof.facetCount', policyDataPath: '$.property.roof.facetCount' },
    { fieldName: 'roofStreaking', propertyDataPath: '$.roof.streaking', policyDataPath: '$.property.roof.streaking' },

    // Cape fields
    { fieldName: 'hasTrampoline', propertyDataPath: '$.hasTrampoline', policyDataPath: '$.property.hasTrampoline' },
    { fieldName: 'hasSwimmingPool', propertyDataPath: '$.hasExteriorSwimmingPool', policyDataPath: '$.property.swimmingPool' }, // reconciled w/ Verisk Valuation

    // Verisk Valuation fields
    { fieldName: 'numberOfStories', propertyDataPath: '$.numberOfStories', policyDataPath: '$.property.numberOfStories' },
    { fieldName: 'squareFootage', propertyDataPath: '$.building.squareFootage', policyDataPath: '$.property.squareFootage' },
    { fieldName: 'yearBuilt', propertyDataPath: '$.yearBuilt', policyDataPath: '$.property.yearBuilt' },
    { fieldName: 'exteriorWallConstruction', propertyDataPath: '$.exteriorWall.construction', policyDataPath: '$.property.exteriorWall.construction' },
    { fieldName: 'exteriorWallFinish', propertyDataPath: '$.exteriorWall.finish', policyDataPath: '$.property.exteriorWall.finish' },
    { fieldName: 'numberOfBedrooms', propertyDataPath: '$.bedroom.count', policyDataPath: '$.property.bedrooms' },
    { fieldName: 'numberOfBathrooms', propertyDataPath: '$.bathroom.count', policyDataPath: '$.property.bathroom.count' },
    { fieldName: 'numberOfFireplaces', propertyDataPath: '$.fireplace.count', policyDataPath: '$.property.fireplaces' },
    { fieldName: 'foundationType', propertyDataPath: '$.foundation.type', policyDataPath: '$.property.foundation.type' },
    { fieldName: 'foundationShape', propertyDataPath: '$.foundation.shape', policyDataPath: '$.property.foundation.shape' },
    { fieldName: 'foundationSlopeAngle', propertyDataPath: '$.foundation.slope', policyDataPath: '$.property.foundation.slope' },
    { fieldName: 'garageType', propertyDataPath: '$.garage.type', policyDataPath: '$.property.garage.type' },
    { fieldName: 'garageSize', propertyDataPath: '$.garage.size', policyDataPath: '$.property.garage.size' },
    { fieldName: 'basementFinishedPercent', propertyDataPath: '$.basement.finishedPercent', policyDataPath: '$.property.basementFinishedPercent' },

    // Verisk Location fields
    { fieldName: 'firelineScore', propertyDataPath: '$.fireRisk.firelineScore', policyDataPath: '$.location.fireRisk.fireLineScore' },
    { fieldName: 'protectionClass', propertyDataPath: '$.fireRisk.protectionClass', policyDataPath: '$.location.fireRisk.protectionClass' },
    { fieldName: 'distanceToFireStation', propertyDataPath: '$.proximity.distanceToFireStation', policyDataPath: '$.location.fireRisk.distanceToFireStation' },
    { fieldName: 'fireDistrictName', propertyDataPath: '$.fireRisk.fireDistrictName', policyDataPath: '$.location.fireRisk.fireDistrictName' },

]

async function main(): Promise<void> {
    loadEnvironmentVariables();
    const numberOfThreads = getNumberOfThreads();
    const threadArray = fill(new Array<number>(numberOfThreads), 0);
    await Promise.all(threadArray.map(() => startThread()));
}

async function startThread(): Promise<void> {
    const localDBConnection = await createSequelizeConnection();

    let shouldContinue: boolean = true;
    while (shouldContinue) {
        shouldContinue = await comparePropertyDataForOnePolicy(localDBConnection);
    }
}

async function comparePropertyDataForOnePolicy(localDBConnection: Sequelize): Promise<boolean> {
    const transaction = await localDBConnection.transaction();
    const tableName = getTableName();

    const getCompareDataQuery = `select `
        + `id, `
        + `policy_data, `
        + `vendor_property_data `
        + `from ${tableName} `
        + `where status = '${StatusEnum.Fetched}' `
        + `limit 1 for update skip locked; `;

    const [ results ] = await localDBConnection.query(getCompareDataQuery, { transaction });

    // do not continue as all rows have been handled
    if (results.length === 0) {
        await transaction.commit();
        return false;

    } else {
        const responseRow = results[0] as Record<string, unknown>;

        const rowId = get(responseRow, 'id') as string;
        const policyData = get(responseRow, 'policy_data') as string;
        const vendorPropertyData = get(responseRow, 'vendor_property_data') as string;

        const vendorPropertyJson = JSON.parse(vendorPropertyData) as Record<string, unknown>;
        const policyJson = JSON.parse(policyData) as Record<string, unknown>;

        const { mismatch, missingOnPolicyData } = comparePropertyData(vendorPropertyJson, policyJson);

        const mismatchJson = JSON.stringify([ ...mismatch ]);
        const missingOnPolicyDataJson = JSON.stringify([ ...missingOnPolicyData ]);

        if (!isEmpty(mismatch)) {
            console.log(`Row ${rowId} has these differences: ${mismatchJson}`);
        }

        if (!isEmpty(missingOnPolicyData)) {
            console.log(`Row ${rowId} has these missing on policy: ${missingOnPolicyDataJson}`);
        }

        const updateSuccessQuery = `update ${tableName} `
            + `set status ='${StatusEnum.Processed}', `
            + ` field_differences = to_jsonb('${mismatchJson}'::text), `
            + ` missing_policy_fields = to_jsonb('${missingOnPolicyDataJson}'::text) `
            + `where id = '${rowId}';`;
        await localDBConnection.query(updateSuccessQuery, { transaction });
        await transaction.commit();

        // script should continue by handling next row
        return true;
    }
}

function comparePropertyData(vendorPropertyData: Record<string, unknown>, policyData: Record<string, unknown>): CompareResponse {
    const mismatch: Set<string> = new Set<string>();
    const missingOnPolicyData: Set<string> = new Set<string>();

    compareFieldPaths.forEach((fieldPaths) => {

        const vendorPropertyValue = JSONPath({ path: fieldPaths.propertyDataPath, json: vendorPropertyData });
        const policyPropertyValue = JSONPath({ path: fieldPaths.policyDataPath, json: policyData });

        // no comparison if both are undefined
        if (!(isNullOrUndefined(vendorPropertyValue[0]) && isNullOrUndefined(policyPropertyValue[0]))) {
            /* A lot of policy values are not being returned even if they exist in vendor-data */
            if (!isNullOrUndefined(vendorPropertyValue[0]) && isNullOrUndefined(policyPropertyValue[0])) {
                missingOnPolicyData.add(fieldPaths.fieldName)

            } else if (
                (isNullOrUndefined(vendorPropertyValue[0]) && !isNullOrUndefined(policyPropertyValue[0])) // vendor does not have data so override was used
                || (!isEqual(vendorPropertyValue[0], policyPropertyValue[0]))
            ) {
                mismatch.add(fieldPaths.fieldName)
            }
        }
    });

    return {
        mismatch,
        missingOnPolicyData
    };
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
