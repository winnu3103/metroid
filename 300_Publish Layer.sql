CREATE OR REPLACE TABLE publish_tbl AS
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
ORDER BY t1.year;

select * from publish_tbl;