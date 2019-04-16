
USE PHClaims;
GO

IF OBJECT_ID('[stage].[v_perf_ah_inpatient_numerator]', 'V') IS NOT NULL
DROP VIEW [stage].[v_perf_ah_inpatient_numerator];
GO
CREATE VIEW [stage].[v_perf_ah_inpatient_numerator]
AS

WITH [get_discharges] AS
(
SELECT 
 [id]
,[tcn]
,[episode_from_date] AS [from_date]
,[episode_to_date] AS [to_date]
,0 AS [observation_stay]
FROM [stage].[v_perf_ah_inpatient_direct_transfer]
WHERE [stay_id] = 1
  AND [death_during_stay] = 0

UNION 

SELECT 
 [id]
,[tcn]
,[from_date]
,[to_date]
,[observation_stay]
FROM [stage].[v_perf_ah_observation_stay]
WHERE [death_during_stay] = 0
)

SELECT 
 ym.[year_month]
,a.[id]
,a.[tcn]
,a.[from_date]
,a.[to_date] 
,a.[observation_stay]
,1 AS [total_discharges]
,CASE WHEN (b.[Surgery] IS NULL AND b.[Surgery MS-DRG] IS NULL) THEN 1 ELSE 0 END AS [medicine]
,CASE WHEN (b.[Surgery] = 1 OR b.[Surgery MS-DRG] = 1) THEN 1 ELSE 0 END AS [surgery]

FROM [get_discharges] AS a

INNER JOIN [ref].[perf_year_month] AS ym
ON a.[to_date] BETWEEN ym.[beg_month] AND ym.[end_month]

LEFT JOIN [stage].[v_perf_ah_medicine_surgery] AS b
ON a.[tcn] = b.[tcn]

WHERE a.[tcn] NOT IN
(
SELECT [tcn] FROM [stage].[v_perf_ah_inpatient_exclusion]
);
GO

/*
Validation Queries

SELECT COUNT(*)
FROM [stage].[v_perf_ah_inpatient_numerator];
*/