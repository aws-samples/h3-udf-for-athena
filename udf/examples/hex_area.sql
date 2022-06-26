
USING EXTERNAL FUNCTION hex_area(res INT, unit VARCHAR) 
RETURNS DOUBLE
LAMBDA '<ARN>'
SELECT hex_area(7, 'km2'),hex_area(7, 'm2')