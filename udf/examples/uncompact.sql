USING EXTERNAL FUNCTION uncompact(h3array ARRAY(BIGINT), res INT)
RETURNS ARRAY(BIGINT)
LAMBDA '<ARN>'
SELECT uncompact(compactedkring, 7) FROM(
    USING EXTERNAL FUNCTION compact(h3array ARRAY(BIGINT))
    RETURNS ARRAY(BIGINT)
    LAMBDA '<ARN>'
    SELECT compact(h3cells) AS compactedkring FROM (
        USING EXTERNAL FUNCTION k_ring(origin BIGINT, k INT) 
        RETURNS ARRAY(BIGINT) 
        LAMBDA '<ARN>'
        SELECT k_ring(599988766760763391, 10) AS h3cells))
