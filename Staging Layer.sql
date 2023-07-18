CREATE DATABASE IF NOT EXISTS metroid;
use database metroid;

CREATE or replace STAGE metstage
URL = 's3://checkingacc';
list @metstage;


CREATE FILE FORMAT my_json_format
  TYPE = 'JSON';

CREATE OR REPLACE TABLE raw_staging (
  metroid VARIANT
);
COPY INTO raw_staging
FROM @metstage/metroids.json
FILE_FORMAT = (FORMAT_NAME = my_json_format);