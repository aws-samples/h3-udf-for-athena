USING EXTERNAL FUNCTION string_to_h3(h3Address VARCHAR) 
RETURNS BIGINT 
LAMBDA 'arn:aws:lambda:eu-west-1:705240738422:function:tlambda'
SELECT  string_to_h3('8a3969ab2037fff')

-- 622506764662964223