USING EXTERNAL FUNCTION h3_unidirectional_edge_is_valid(edge BIGINT)
RETURNS BOOLEAN
LAMBDA '<ARN>'
SELECT h3_unidirectional_edge_is_valid(100), h3_unidirectional_edge_is_valid(1248507113102114815)
