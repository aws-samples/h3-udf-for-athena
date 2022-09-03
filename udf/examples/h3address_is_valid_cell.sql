USING EXTERNAL FUNCTION is_valid_cell(h3Address VARCHAR) 
RETURNS BOOLEAN
LAMBDA '<ARN>'
SELECT is_valid_cell('2'), is_valid_cell('8a3969ab2037fff')