USING EXTERNAL FUNCTION get_origin_destination_h3_index_from_unidirectional_edge(edge VARCHAR)
RETURNS ARRAY(VARCHAR)
LAMBDA '<ARN>'
SELECT get_origin_destination_h3_index_from_unidirectional_edge('16a3969ab218ffff') 