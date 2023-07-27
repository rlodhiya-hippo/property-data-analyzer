# Property Data Analyzer Fix

## What this script does

The script does the following things:

1. Queries properties from a temporary table containing Policies and associated Property from a specified time duration

2. For each of the above rows, retrieves property-data and policy-data. 

3. TODO: finish the script

4. Each script is a `worker` that can be run in parallel in different nodes (terminals) locally.


## Prerequisites for this script

1. Create local DB with `docker compose up -d postgres`

2. Run `local-db-initialize.sql` on local db

3. Run DWH query using `dwh-query.sql`, and import results into local DB

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

## Running the script

1. Run `yarn build:clean`
2. `node dist/index.js`
3. Check logs
