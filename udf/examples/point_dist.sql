USING EXTERNAL FUNCTION point_dist(pointWKT1 VARCHAR, pointWKT2 VARCHAR, unit VARCHAR) 
RETURNS DOUBLE
LAMBDA '<ARN>'
SELECT point_dist(ST_AsText(ST_Point(43.552847, 7.017369)),
                  ST_AsText(ST_Point(47.218371, -1.55362)),'km')