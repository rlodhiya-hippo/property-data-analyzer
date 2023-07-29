# Property Data Analyzer

## What this app does

There are 3 scripts that do the following things:

1. Queries properties from a temporary table containing Policies and associated Property from a specified time duration

2. For each of row, retrieves property-data and policy-data, and persists them to local DB

3. Goes over each row with policy and property data, and compare them using known field mappings

4. Persists the differences for each row of policy and property data

5. 


## Prerequisites

1. Create local DB with `docker compose up -d postgres` (alternatively, a local postgres DB can be used)

2. Run `local-db-initialize.sql` on local db

3. Run DWH query using `dwh-query.sql`, and import results into local DB (import has to be done manually)

4. The following environment variables need to be set in order to run the script
    ```shell
    PROPERTY_SERVICE_V1_BASE_URL
    PROPERTY_SERVICE_V1_USERNAME
    PROPERTY_SERVICE_V1_PASSWORD
    
    POLICY_SERVICE_V1_BASE_URL
    POLICY_SERVICE_V1_USERNAME
    POLICY_SERVICE_V1_PASSWORD
      
    SEQUELIZE_HOST
    SEQUELIZE_USERNAME
    SEQUELIZE_PASSWORD
    SEQUELIZE_DATABASE
    SEQUELIZE_PORT
    SEQUELIZE_TABLENAME
    ```

## Running

1. Run `yarn install && yarn build:clean`
2. `node dist/retrieve-property-data.js --numberOfThreads 1`
3. `node dist/compare-property-data.js --numberOfThreads 1`
4. `node dist/compute-diff-statistics.js`
