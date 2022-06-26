USING EXTERNAL FUNCTION h3_unidirectional_edge_is_valid(edge VARCHAR)
RETURNS BOOLEAN
LAMBDA 'ARN'
SELECT h3_unidirectional_edge_is_valid('100') valid1, h3_unidirectional_edge_is_valid('16a3969ab218ffff') valid2
-- false, true