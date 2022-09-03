USING EXTERNAL FUNCTION is_valid_directed_edge(edge VARCHAR)
RETURNS BOOLEAN
LAMBDA '<ARN>'
SELECT is_valid_directed_edge('100') valid1, is_valid_directed_edge('16a3969ab218ffff') valid2
-- false, true