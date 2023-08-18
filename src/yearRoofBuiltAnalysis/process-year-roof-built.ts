import { Sequelize } from 'sequelize-typescript';
import { fill, find, first, get } from 'lodash';
import { JSONPath } from 'jsonpath-plus';

import {
    StatusEnum,
    createSequelizeConnection,
    getNumberOfThreads,
    getTableName,
    loadEnvironmentVariables,
} from './common';

class FieldPath {
    fieldName!: string;
    propertyDataPath!: string;
    policyDataPath!: string;
}

class ParseRoofDataResponse {
    vendorYearRoofBuilt?: number;
    policyYearRoofBuilt?: number;
    vendorRoofCondition?: string
    roofAgeCustomer?: number;
    roofAgeVendor?: number;
    roofAgeDiff?: number;
}

const compareFieldPaths: FieldPath[] = [

    // check for sanity
    { fieldName: 'propertyRevision', propertyDataPath: '$.revision', policyDataPath: '$.property.revision' },

    // Verisk Valuation Roof fields
    { fieldName: 'yearRoofBuilt', propertyDataPath: '$.roof.yearBuilt', policyDataPath: '$.property.roof.yearBuilt' },

    // Cape Roof fields
    { fieldName: 'roofConditionRating', propertyDataPath: '$.roof.conditionRating', policyDataPath: '$.property.roof.conditionRating' },
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
        + `policy_bound_date, `
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
        const policyBoundDate = get(responseRow, 'policy_bound_date') as string;
        const policyData = get(responseRow, 'policy_data') as Record<string, unknown>;
        const vendorPropertyData = get(responseRow, 'vendor_property_data') as Record<string, unknown>;

        const boundYear = new Date(policyBoundDate).getFullYear();

        const {
            vendorYearRoofBuilt,
            policyYearRoofBuilt,
            vendorRoofCondition,
            roofAgeCustomer,
            roofAgeVendor,
            roofAgeDiff,
        } = parseRoofData(vendorPropertyData, policyData, boundYear);

        const updateSuccessQuery = `update ${tableName} `
            + `set status ='${StatusEnum.Processed}', `
            + ` vendor_year_roof_built = :vendorYearRoofBuilt , `
            + ` policy_year_roof_built = :policyYearRoofBuilt , `
            + ` vendor_roof_condition = :vendorRoofCondition ,`
            + ` roof_age_customer = :roofAgeCustomer , `
            + ` roof_age_vendor = :roofAgeVendor ,`
            + ` diff_roof_age_customer_vendor = :roofAgeDiff `
            + `where id = '${rowId}';`;
        await localDBConnection.query(updateSuccessQuery, {
            transaction,
            replacements: {
                vendorYearRoofBuilt: vendorYearRoofBuilt ?? null,
                policyYearRoofBuilt: policyYearRoofBuilt ?? null,
                vendorRoofCondition: vendorRoofCondition ?? null,
                roofAgeCustomer: roofAgeCustomer ?? null,
                roofAgeVendor: roofAgeVendor ?? null,
                roofAgeDiff: roofAgeDiff ?? null,
            },
        });
        await transaction.commit();

        // script should continue by handling next row
        return true;
    }
}

function parseRoofData(vendorPropertyData: Record<string, unknown>, policyData: Record<string, unknown>, boundYear: number): ParseRoofDataResponse {
    let vendorYearRoofBuilt: number | undefined;
    let policyYearRoofBuilt: number | undefined;
    let vendorRoofCondition: string | undefined;
    let roofAgeCustomer: number | undefined;
    let roofAgeVendor: number | undefined;
    let roofAgeDiff: number | undefined;

    const yearRoofBuiltPath: FieldPath | undefined = find(compareFieldPaths, (fieldPath) => fieldPath.fieldName === 'yearRoofBuilt');
    if (yearRoofBuiltPath) {
        vendorYearRoofBuilt = first(JSONPath({ path: yearRoofBuiltPath.propertyDataPath, json: vendorPropertyData })) as number | undefined;
        policyYearRoofBuilt = first(JSONPath({ path: yearRoofBuiltPath.policyDataPath, json: policyData })) as number | undefined;
        if (policyYearRoofBuilt) {
            roofAgeCustomer = boundYear - policyYearRoofBuilt;
        }
        if (vendorYearRoofBuilt) {
            roofAgeVendor = boundYear - vendorYearRoofBuilt;
        }
        if (policyYearRoofBuilt && vendorYearRoofBuilt) {
            roofAgeDiff = policyYearRoofBuilt - vendorYearRoofBuilt;
        }
    }

    const roofConditionPath: FieldPath | undefined = find(compareFieldPaths, (fieldPath) => fieldPath.fieldName === 'roofConditionRating');
    if (roofConditionPath) {
        vendorRoofCondition = first(JSONPath({ path: roofConditionPath.propertyDataPath, json: vendorPropertyData })) as string | undefined;
    }

    return {
        vendorYearRoofBuilt,
        policyYearRoofBuilt,
        vendorRoofCondition,
        roofAgeCustomer,
        roofAgeVendor,
        roofAgeDiff,
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
