// Создание обьекта storage integration для AWS S3
CREATE OR REPLACE STORAGE INTEGRATION s3_integr
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::083226907131:role/snowflake-access-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://snowflakeelts3bucket/json_folder');

//Для настройки policy
DESC INTEGRATION s3_integr;

//Создание новой схемы и таблицы для данных из S3
CREATE OR REPLACE SCHEMA S3;

CREATE OR REPLACE TABLE raw_table (
    player_id STRING,
    device_id STRING,
    install_date DATETIME,
    client_id STRING,
    app_name STRING,
    country STRING);

// Создание обьекта file format для json
CREATE OR REPLACE file format json_fileformat
    TYPE = json

//Создание объекта stage 
CREATE OR REPLACE stage stg_json_folder
    URL = 's3://snowflakeelts3bucket/json_folder'
    STORAGE_INTEGRATION = s3_integr
    FILE_FORMAT = json_fileformat;

//Создаем и заполняем временную таблицу данными из S3
CREATE OR REPLACE TEMPORARY TABLE tmp_s3_table AS
SELECT
    $1:event_data:data:eventData:app_user_id AS player_id,
    $1:event_data:data:eventData:platformAccountId AS device_id,
    $1:event_data:timestampClient::datetime AS install_date,
    $1:event_data:platform AS client_id,
    $1:event_data:appName AS app_name,
    $1:event_data:countryCode AS country
FROM @stg_json_folder
WHERE $1:event_data:data:eventData:eventType = 'server_install';

//Заполняем ранее созданную таблицу
INSERT INTO raw_table
SELECT * FROM tmp_s3_table;

//Удаляем временную таблицу
DROP TABLE IF EXISTS tmp_s3_table;














    
