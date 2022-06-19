USING EXTERNAL FUNCTION h3_to_geo_boundary_sys(h3 BIGINT, coordsys VARCHAR)
RETURNS ARRAY(DOUBLE)
LAMBDA '<ARN>'
SELECT zip_with(
       h3_to_geo_boundary_sys(h3_cell, 'lat'),
       h3_to_geo_boundary_sys(h3_cell, 'lng'),
       (x, y)-> ST_Point(x,y)) as coord 
FROM(
USING EXTERNAL FUNCTION geo_to_h3(lat DOUBLE, lng DOUBLE, res INTEGER)
RETURNS BIGINT
LAMBDA '<ARN>'
  SELECT geo_to_h3(latitude, longitude, 4) AS h3_cell
  FROM earthquakes
  WHERE latitude BETWEEN 18 AND 70)