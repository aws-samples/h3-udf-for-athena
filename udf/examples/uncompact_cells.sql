USING EXTERNAL FUNCTION uncompact_cells(h3array ARRAY(BIGINT), res INT)
RETURNS ARRAY(BIGINT)
LAMBDA '<ARN>'
SELECT uncompact_cells(compactedkring, 7) FROM(
    USING EXTERNAL FUNCTION compact_cells(h3array ARRAY(BIGINT))
    RETURNS ARRAY(BIGINT)
    LAMBDA '<ARN>'
    SELECT compact_cells(h3cells) AS compactedkring   FROM (
        USING EXTERNAL FUNCTION grid_disk(origin BIGINT, k INT) 
        RETURNS ARRAY(BIGINT) 
        LAMBDA '<ARN>'
        SELECT grid_disk(599988766760763391, 10) AS h3cells))
        