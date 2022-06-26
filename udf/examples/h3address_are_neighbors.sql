USING EXTERNAL FUNCTION h3_indexes_are_neighbors(origin VARCHAR, destination VARCHAR)
RETURNS BOOLEAN
LAMBDA '<ARN>'
SELECT h3_indexes_are_neighbors('8a3969ab20affff', '8a3969ab218ffff') 