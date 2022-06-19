USING EXTERNAL FUNCTION h3_is_pentagon(h3addr VARCHAR) 
RETURNS BOOLEAN
LAMBDA '<ARN>'
SELECT h3_is_pentagon('851c0003fffffff'), h3_is_pentagon('8a3969ab2037fff');