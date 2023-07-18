CREATE OR REPLACE TABLE other_tbl (
  id INT,
  name STRING,
  nametype STRING,
  recclass STRING,
  mass STRING,
  fall STRING,
  year TIMESTAMP,
  reclat STRING,
  reclong STRING,
  PRIMARY KEY (id)
);


CREATE OR REPLACE TABLE geolocation_tbl (
  id INT,
  type STRING,
  latitude FLOAT,
  longitude FLOAT,
  PRIMARY KEY (id),
  FOREIGN KEY (id) REFERENCES other_tbl(id)
);

INSERT INTO geolocation_tbl (id, type, latitude, longitude)
SELECT metroid:id::INT, metroid:geolocation:type, metroid:geolocation:coordinates[1]::FLOAT, metroid:geolocation:coordinates[0]::FLOAT
FROM raw_tbl;

select * from geolocation_tbl;

INSERT INTO other_tbl (id, name, nametype, recclass, mass, fall, year, reclat, reclong)
SELECT metroid:id::INT, metroid:name, metroid:nametype, metroid:recclass, metroid:mass,
       metroid:fall, TO_TIMESTAMP_NTZ(metroid:year::STRING), metroid:reclat, metroid:reclong
FROM raw_tbl;

select * from other_tbl;