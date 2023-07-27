--Publish layer
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

--trend analysis for the 'recclass' for each 'year'
SELECT year, recclass, COUNT(*) as count
FROM publish_tbl
GROUP BY year, recclass
ORDER BY year;

--trend analysis with % growth of 'mass' for each year
WITH yearly_mass AS (
  SELECT year, SUM(mass) as total_mass
  FROM publish_tbl
  GROUP BY year
)
SELECT 
  current_year.year, 
  current_year.total_mass as current_year_mass, 
  previous_year.total_mass as previous_year_mass, 
  (current_year.total_mass - previous_year.total_mass) / previous_year.total_mass * 100 as mass_growth_percentage
FROM yearly_mass current_year
LEFT JOIN yearly_mass previous_year ON current_year.year - INTERVAL '1 YEAR' = previous_year.year
ORDER BY current_year.year;
