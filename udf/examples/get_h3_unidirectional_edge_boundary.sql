USING EXTERNAL FUNCTION get_h3_unidirectional_edge_boundary(edge BIGINT) 
RETURNS ARRAY(VARCHAR) 
LAMBDA '<ARN>'
SELECT get_h3_unidirectional_edge_boundary(1248507113102114815)