USING EXTERNAL FUNCTION cell_to_lat_lng_wkt(h3 BIGINT) 
RETURNS VARCHAR
LAMBDA '<ARN>'
SELECT ST_GeometryFromText(cell_to_lat_lng_wkt(622506764662964223)) as point