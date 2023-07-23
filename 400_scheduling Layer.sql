CREATE OR REPLACE PIPE snowpipe_raw_staging
AUTO_INGEST = TRUE
AS
COPY INTO raw_staging (metroid)
FROM @metstage
FILE_FORMAT = my_json_format;

ALTER PIPE snowpipe_raw_staging REFRESH;


CREATE OR REPLACE STREAM raw_staging_stream ON TABLE raw_staging;

CREATE OR REPLACE TASK task_load_raw_tbl
WAREHOUSE = METROIDS
SCHEDULE = 'USING CRON 0 9-17 * * 1-5 UTC'
WHEN SYSTEM$STREAM_HAS_DATA('raw_staging_stream')
AS
INSERT INTO raw_tbl (metroid)
SELECT VALUE metroid
FROM raw_staging_stream,
LATERAL FLATTEN(input => parse_json(raw_staging_stream.metroid));

CREATE OR REPLACE STREAM raw_tbl_stream ON TABLE raw_tbl;

CREATE OR REPLACE TASK task_load_other_tbl
AFTER task_load_raw_tbl
WHEN (SYSTEM$STREAM_HAS_DATA('raw_tbl_stream'))
AS
INSERT INTO other_tbl (id, name, nametype, recclass, mass, fall, year, reclat, reclong)
SELECT metroid:id::INT, metroid:name, metroid:nametype, metroid:recclass, metroid:mass, metroid:fall, TO_TIMESTAMP_NTZ(metroid:year::STRING), metroid:reclat, metroid:reclong
FROM (SELECT metroid FROM raw_tbl_stream);

CREATE OR REPLACE TASK task_load_geolocation_tbl
AFTER task_load_raw_tbl
WHEN SYSTEM$STREAM_HAS_DATA('raw_tbl_stream')
AS
INSERT INTO geolocation_tbl (id, type, latitude, longitude)
SELECT metroid:id::INT, metroid:geolocation:type, metroid:geolocation:coordinates[1]::FLOAT, metroid:geolocation:coordinates[0]::FLOAT
FROM (SELECT metroid FROM raw_tbl_stream);

CREATE OR REPLACE STREAM other_tbl_stream ON TABLE other_tbl;

CREATE OR REPLACE TASK task_update_publish_tbl
AFTER task_load_other_tbl
WHEN SYSTEM$STREAM_HAS_DATA('other_tbl_stream')
AS
INSERT INTO publish_tbl (id, name, nametype, recclass, mass, fall, year, reclat, reclong, previous_mass, mass_growth_percentage)
SELECT 
  t1.id,
  t1.name,
  t1.nametype,
  t1.recclass,
  t1.mass,
  t1.fall,
  t1.year,
  t1.reclat,
  t1.reclong,
  t2.mass AS previous_mass,
  CASE 
    WHEN t2.mass IS NULL THEN 0.0
    ELSE (t1.mass - t2.mass) / t2.mass * 100.0
  END AS mass_growth_percentage
FROM other_tbl t1
LEFT JOIN other_tbl t2 ON DATE_TRUNC('YEAR', t1.year) - INTERVAL '1 YEAR' = DATE_TRUNC('YEAR', t2.year)
WHERE t1.year IN (
  SELECT metroid:year::TIMESTAMP_NTZ
  FROM (SELECT metroid FROM raw_tbl_stream)
);