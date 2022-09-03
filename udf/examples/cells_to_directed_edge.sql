USING EXTERNAL FUNCTION cells_to_directed_edge(origin VARCHAR, destination VARCHAR)
RETURNS VARCHAR
LAMBDA '<ARN>'
SELECT cells_to_directed_edge('8a3969ab218ffff', '8a3969ab2187fff')  AS edge