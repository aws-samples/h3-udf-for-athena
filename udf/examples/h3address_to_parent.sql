USING EXTERNAL FUNCTION cell_to_parent(h3addr VARCHAR, parentres INT) 
RETURNS VARCHAR 
LAMBDA '<ARN>'
SELECT cell_to_parent('853969abfffffff', 4)