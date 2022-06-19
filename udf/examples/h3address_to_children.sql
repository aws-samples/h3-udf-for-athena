USING EXTERNAL FUNCTION h3_to_children(h3 VARCHAR, childres INT) 
RETURNS ARRAY(VARCHAR) 
LAMBDA '<ARN>'
SELECT h3_to_children('843969bffffffff', 5)
-- [853969a3fffffff, 853969a7fffffff, 853969abfffffff, 853969affffffff, 853969b3fffffff, 853969b7fffffff, 853969bbfffffff]