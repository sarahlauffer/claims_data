
USE PHClaims;
GO

IF OBJECT_ID('[stage].[mcaid_claim_value_set]') IS NOT NULL
DROP TABLE [stage].[mcaid_claim_value_set];
CREATE TABLE [stage].[mcaid_claim_value_set]
([version] SMALLINT NOT NULL
,[value_set_name] VARCHAR(100) NOT NULL
,[code_system] VARCHAR(50) NOT NULL
,[primary_dx_only] CHAR(1) NOT NULL
,[id_mcaid] VARCHAR(255) NOT NULL
,[claim_header_id] BIGINT NOT NULL
,[first_service_date] DATE NOT NULL
,[last_service_date] DATE NOT NULL);
GO

INSERT INTO [stage].[mcaid_claim_value_set]
SELECT DISTINCT
 hed.[version]
,hed.[value_set_name]
,hed.[code_system]
,'Y' AS [primary_dx_only]
,dx.[id_mcaid]
,dx.[claim_header_id]
,dx.[first_service_date]
,dx.[last_service_date]
FROM [final].[mcaid_claim_icdcm_header] AS dx
INNER JOIN [archive].[hedis_code_system] AS hed
ON hed.[value_set_name] IN 
('AOD Abuse and Dependence'
,'Mental Health Diagnosis'
,'Mental Illness')
AND hed.[code_system] = 'ICD10CM'
AND dx.[icdcm_version] = 10
-- Principal Diagnosis
AND dx.[icdcm_number] = '01'
AND dx.[icdcm_norm] = hed.[code];

INSERT INTO [stage].[mcaid_claim_value_set]
SELECT DISTINCT
 hed.[version]
,hed.[value_set_name]
,hed.[code_system]
,'N' AS [primary_dx_only]
,ln.[id_mcaid]
,ln.[claim_header_id]
,ln.[first_service_date]
,ln.[last_service_date]
FROM [final].[mcaid_claim_line] AS ln
INNER JOIN [archive].[hedis_code_system] AS hed
ON hed.[value_set_name] IN 
('ED')
AND hed.[code_system] = 'UBREV'
AND ln.[rev_code] = hed.[code];

INSERT INTO [stage].[mcaid_claim_value_set]
SELECT DISTINCT
 hed.[version]
,hed.[value_set_name]
,hed.[code_system]
,'N' AS [primary_dx_only]
,pr.[id_mcaid]
,pr.[claim_header_id]
,pr.[first_service_date]
,pr.[last_service_date]
FROM [final].[mcaid_claim_procedure] AS pr
INNER JOIN [archive].[hedis_code_system] AS hed
ON hed.[value_set_name] IN 
('ED')
AND hed.[code_system] = 'CPT'
AND pr.[procedure_code] = hed.[code];

/*
SELECT TOP(100) * FROM [stage].[mcaid_claim_value_set];
SELECT COUNT(*) FROM [stage].[mcaid_claim_value_set];
*/