USING EXTERNAL FUNCTION geo_to_h3_address(lat DOUBLE, lng DOUBLE, res INT) 
RETURNS VARCHAR 
LAMBDA '<ARN>'
SELECT geo_to_h3_address(43.552847, 7.017369, 10)