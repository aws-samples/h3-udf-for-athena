USING EXTERNAL FUNCTION h3_to_parent(h3 BIGINT, parentres INT) 
RETURNS BIGINT 
LAMBDA '<ARN>'
SELECT h3_to_parent(599988766760763391, 4)
-- 595485172502102015
