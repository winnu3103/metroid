--creating raw table
CREATE OR REPLACE TABLE raw_tbl (
  metroid VARIANT
);

-- inserting into table1
INSERT INTO raw_tbl (metroid)
SELECT VALUE metroid
FROM raw_staging,
LATERAL FLATTEN(input => parse_json(raw_staging.metroid));

select * from raw_tbl;
