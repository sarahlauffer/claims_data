
USE [PHClaims];
GO

IF OBJECT_ID('[stage].[v_perf_ah_inpatient_exclusion]', 'V') IS NOT NULL
DROP VIEW [stage].[v_perf_ah_inpatient_exclusion];
GO
CREATE VIEW [stage].[v_perf_ah_inpatient_exclusion]
AS

WITH [get_all_exclusions] AS
( 
SELECT 
 hd.[id]
,hd.[tcn]
,hed.[value_set_name]
,1 AS [flag]

FROM [dbo].[mcaid_claim_header] AS hd
INNER JOIN [ref].[hedis_code_system] AS hed
ON [value_set_name] IN 
('IPU Exclusions MS-DRG'
,'Maternity MS-DRG'
,'Newborns/Neonates MS-DRG')
AND hed.[code_system] = 'MSDRG' 
AND hd.[drg_code] = hed.[code]

UNION

SELECT 
 hd.[id]
,hd.[tcn]
,hed.[value_set_name]
,1 AS [flag]

FROM [dbo].[mcaid_claim_header] AS hd
INNER JOIN [ref].[hedis_code_system] AS hed
ON [value_set_name] IN 
('Maternity')
AND hed.[code_system] = 'UBTOB' 
AND hd.[bill_type_code] = hed.[code]

UNION

SELECT 
 dx.[id]
,dx.[tcn]
,hed.[value_set_name]
,1 AS [flag]

FROM [dbo].[mcaid_claim_dx] AS dx
INNER JOIN [ref].[hedis_code_system] AS hed
ON [value_set_name] IN 
('Deliveries Infant Record'
,'Maternity Diagnosis'
,'Mental and Behavioral Disorders')
AND hed.[code_system] = 'ICD9CM'
AND dx.[dx_ver] = 9 
-- Principal Diagnosis
AND dx.[dx_number] = 1
AND dx.[dx_norm] = hed.[code]

UNION

SELECT 
 dx.[id]
,dx.[tcn]
,hed.[value_set_name]
,1 AS [flag]

FROM [dbo].[mcaid_claim_dx] AS dx
INNER JOIN [ref].[hedis_code_system] AS hed
ON [value_set_name] IN 
('Deliveries Infant Record'
,'Maternity Diagnosis'
,'Mental and Behavioral Disorders')
AND hed.[code_system] = 'ICD10CM'
AND dx.[dx_ver] = 10 
-- Principal Diagnosis
AND dx.[dx_number] = 1
AND dx.[dx_norm] = hed.[code]

UNION

SELECT 
 ln.[id]
,ln.[tcn]
,hed.[value_set_name]
,1 AS [flag]

FROM [dbo].[mcaid_claim_line] AS ln
INNER JOIN [ref].[hedis_code_system] AS hed
ON [value_set_name] IN 
('Maternity')
AND hed.[code_system] = 'UBREV'
AND ln.[rcode] = hed.[code]
)

SELECT 
 [id]
,[tcn]
,[Deliveries Infant Record]
,[IPU Exclusions MS-DRG]
,[Maternity]
,[Maternity Diagnosis]
,[Maternity MS-DRG]
,[Mental and Behavioral Disorders]
,[Newborns/Neonates MS-DRG]
FROM [get_all_exclusions]
PIVOT(MAX([flag]) FOR [value_set_name] IN
(
 [Deliveries Infant Record]
,[IPU Exclusions MS-DRG]
,[Maternity]
,[Maternity Diagnosis]
,[Maternity MS-DRG]
,[Mental and Behavioral Disorders]
,[Newborns/Neonates MS-DRG]
)) AS P;
GO

/*
SELECT COUNT(*) FROM [stage].[v_perf_ah_inpatient_exclusion];
*/