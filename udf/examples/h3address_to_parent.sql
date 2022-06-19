USING EXTERNAL FUNCTION h3_to_parent(h3addr VARCHAR, parentres INT) 
RETURNS VARCHAR 
LAMBDA '<ARN>'
SELECT h3_to_parent('853969abfffffff', 4)
-- 843969bffffffff