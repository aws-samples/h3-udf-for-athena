USING EXTERNAL FUNCTION uncompact_cell_addresses(h3array ARRAY(VARCHAR), res INT)
RETURNS ARRAY(VARCHAR)
LAMBDA '<ARN>'
SELECT uncompact_cell_addresses(compactedkring, 7) FROM(
    USING EXTERNAL FUNCTION compact_cell_addresses(h3array ARRAY(VARCHAR))
    RETURNS ARRAY(VARCHAR)
    LAMBDA '<ARN>'
    SELECT compact_cell_addresses(h3cells) AS compactedkring FROM (
        USING EXTERNAL FUNCTION grid_disk(origin VARCHAR, k INT) 
        RETURNS ARRAY(VARCHAR) 
        LAMBDA '<ARN>'
        SELECT grid_disk('853969abfffffff', 10) AS h3cells))