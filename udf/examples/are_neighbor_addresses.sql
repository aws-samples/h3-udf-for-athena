USING EXTERNAL FUNCTION are_neighbor_cells(origin VARCHAR, destination VARCHAR)
RETURNS BOOLEAN
LAMBDA '<ARN>'
SELECT '8a3969ab20affff' as cell1, '8a3969ab218ffff' as cell2, are_neighbor_cells('8a3969ab20affff', '8a3969ab218ffff') as neighbor
