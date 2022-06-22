USING EXTERNAL FUNCTION uncompact_address(h3array ARRAY(VARCHAR), res INT)
RETURNS ARRAY(VARCHAR)
LAMBDA 'arn:aws:lambda:<region>:<account>:function:<lambdaname>'
SELECT uncompact_address(compactedkring, 7) FROM(
    USING EXTERNAL FUNCTION compact_address(h3array ARRAY(VARCHAR))
    RETURNS ARRAY(VARCHAR)
    LAMBDA 'arn:aws:lambda:<region>:<account>:function:<lambdaname>'
    SELECT compact_address(h3cells) AS compactedkring FROM (
        USING EXTERNAL FUNCTION k_ring(origin VARCHAR, k INT) 
        RETURNS ARRAY(VARCHAR) 
        LAMBDA 'arn:aws:lambda:<region>:<account>:function:<lambdaname>'
        SELECT k_ring('853969abfffffff', 10) AS h3cells))
