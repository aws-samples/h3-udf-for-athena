USING EXTERNAL FUNCTION cell_to_lat_lng_wkt(h3Address VARCHAR) 
RETURNS VARCHAR
LAMBDA '<ARN>'
SELECT ST_GeometryFromText(cell_to_lat_lng_wkt('8a3969ab2037fff'))

-- POINT (7.017036 43.5528)
