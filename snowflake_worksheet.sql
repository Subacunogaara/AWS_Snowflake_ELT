    // Creating storage integration for AWS S3
CREATE OR REPLACE STORAGE INTEGRATION s3_integr
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::**********:role/snowflake-access-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://snowflakeelts3bucket/json_folder');

    //For setting policy
DESC INTEGRATION s3_integr;

    //Creating a new schema for data from S3
CREATE OR REPLACE SCHEMA S3;

    //Creating file format for json files
CREATE OR REPLACE file format json_fileformat
    TYPE = json

    
    //Creating stage object for json files
    //with integration object and file format
CREATE OR REPLACE stage stg_json_folder
    URL = 's3://snowflakeelts3bucket/json_folder/snowpipe'
    STORAGE_INTEGRATION = s3_integr
    FILE_FORMAT = json_fileformat;

    //For checking the information inside the bucket
LIST @stg_json_folder;


    //Creating a table for raw data
CREATE OR REPLACE TABLE raw_data_table(
raw_data VARIANT);


    //Creating pipe object
CREATE OR REPLACE PIPE json_pipe
auto_ingest = TRUE
AS
COPY INTO raw_data_table
FROM @stg_json_folder;

    //For sitting up notiffications
DESC PIPE json_pipe;


    //Creating the final table for clean data
CREATE OR REPLACE TABLE server_install_table(
player_id VARCHAR,
device_id VARCHAR,
install_date DATETIME,
client_id VARCHAR,
app_name VARCHAR,
country VARCHAR
);

    //Creating stream for raw data table
CREATE OR REPLACE STREAM json_stream ON TABLE raw_data_table;

    //Creating task for reading stream object  
CREATE OR REPLACE TASK read_stream
WAREHOUSE = COMPUTE_WH
SCHEDULE = '5 MINUTE'
AS
INSERT INTO server_install_table(
SELECT
    $1:event_data:data:eventData:app_user_id AS player_id,
    $1:event_data:data:eventData:platformAccountId AS device_id,
    $1:event_data:timestampClient::datetime AS install_date,
    $1:event_data:platform AS client_id,
    $1:event_data:appName AS app_name,
    $1:event_data:countryCode AS country
FROM json_stream
WHERE $1:event_data:data:eventData:eventType = 'server_install'
);

    //Starting and suspending task
ALTER TASK read_stream RESUME;
ALTER TASK read_stream SUSPEND;
