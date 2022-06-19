USING EXTERNAL FUNCTION h3_to_center_child(h3 BIGINT, childres INT) 
RETURNS BIGINT
LAMBDA 'arn:aws:lambda:eu-west-1:705240738422:function:tlambda'
SELECT h3_to_center_child(595485172502102015, 5)