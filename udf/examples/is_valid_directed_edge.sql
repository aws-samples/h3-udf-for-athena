USING EXTERNAL FUNCTION is_valid_directed_edge(edge BIGINT)
RETURNS BOOLEAN
LAMBDA '<ARN>'
SELECT is_valid_directed_edge(100), is_valid_directed_edge(1248507113102114815)
