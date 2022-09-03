
USING EXTERNAL FUNCTION cell_area(res INT, unit VARCHAR) 
RETURNS DOUBLE
LAMBDA '<ARN>'
SELECT cell_area(7, 'km2'),cell_area(7, 'm2')