
USE [PHClaims];
GO

IF OBJECT_ID('[stage].[sp_load_mcaid_periodic_snapshot]','P') IS NOT NULL
DROP PROCEDURE [stage].[sp_load_mcaid_periodic_snapshot];
GO
CREATE PROCEDURE [stage].[sp_load_mcaid_periodic_snapshot]
AS
SET NOCOUNT ON;

BEGIN

TRUNCATE TABLE [stage].[mcaid_periodic_snapshot];

DECLARE 
 @start_year_quarter INT = 201201
,@end_year_quarter INT = 201902;

IF OBJECT_ID('tempdb..#year_quarter') IS NOT NULL
DROP TABLE #year_quarter;
SELECT DISTINCT
 [quarter]
,[quarter_name]
,[year_quarter]
,[first_day_quarter]
,[last_day_quarter]
INTO #year_quarter
FROM [ref].[date]
WHERE [year_quarter] BETWEEN @start_year_quarter AND @end_year_quarter;
CREATE CLUSTERED INDEX idx_cl_#year_quarter
ON #year_quarter([first_day_quarter], [last_day_quarter]);

IF OBJECT_ID('tempdb..#year_month') IS NOT NULL
DROP TABLE #year_month;
SELECT DISTINCT
 [year_month]
,[year_quarter]
INTO #year_month
FROM [ref].[date]
WHERE [month] IN (3, 6, 9, 12);
CREATE CLUSTERED INDEX idx_cl_#year_month
ON #year_month([year_quarter]);

/*
Step 1:
JOIN [final].[mcaid_claim_icdcm_header] to [ref].[comorb_value_set] to [ref].[date]
and GROUP BY [year_quarter], [id_mcaid], [cond_id].
This table will have an Indicator/Weight for Person by Quarter by Condition.
*/
IF OBJECT_ID('tempdb..#comorb_value_set') IS NOT NULL
DROP TABLE #comorb_value_set;
SELECT
--TOP(1000)
 [year_quarter]
,[id_mcaid]
,[cond_id]
,[elixhauser_wgt]
,[charlson_wgt]
,[gagne_wgt]
,SUM([flag]) AS [flag]
INTO #comorb_value_set
FROM [final].[mcaid_claim_icdcm_header] AS a
INNER JOIN [ref].[comorb_value_set] AS b
ON a.[icdcm_version] = b.[dx_ver]
AND a.[icdcm_norm] = b.[dx]
INNER JOIN #year_quarter AS c
ON a.[first_service_date] BETWEEN c.[first_day_quarter] AND c.[last_day_quarter]
GROUP BY
 [year_quarter]
,[id_mcaid]
,[cond_id]
,[elixhauser_wgt]
,[charlson_wgt]
,[gagne_wgt];
CREATE CLUSTERED INDEX idx_cl_#comorb_value_set 
ON #comorb_value_set([id_mcaid], [cond_id], [year_quarter]);

/*
Step 2:
In order to calculate scores within rolling 4-quarter windows,
it is necessary to create empty/placeholder [year_quarter] rows
for [id_mcaid] by [cond_id] combinations.

This enables use of the window function below:
MAX([elixhauser_wgt]) OVER(PARTITION BY [id_mcaid], [cond_id] ORDER BY [year_quarter] ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS [elixhauser_t_12_m]

The empty/placeholder [year_quarter] rows are necessary for 
ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
to work properly.

The empty/placeholder [year_quarter] rows are created by
(SELECT DISTINCT [id_mcaid], [cond_id] FROM #comorb_value_set) CROSS JOIN #date LEFT JOIN #comorb_value_set
*/

IF OBJECT_ID('tempdb..#year_quarter_id_mcaid_cond_id') IS NOT NULL
DROP TABLE #year_quarter_id_mcaid_cond_id;
SELECT
 b.[year_quarter]
,a.[id_mcaid]
,a.[cond_id]
,c.[elixhauser_wgt]
,c.[charlson_wgt]
,c.[gagne_wgt]
INTO #year_quarter_id_mcaid_cond_id
FROM (SELECT DISTINCT [id_mcaid], [cond_id] FROM #comorb_value_set) AS a
CROSS JOIN #year_quarter AS b
LEFT JOIN #comorb_value_set AS c
ON a.[id_mcaid] = c.[id_mcaid]
AND a.[cond_id] = c.[cond_id]
AND b.[year_quarter] = c.[year_quarter];
CREATE CLUSTERED INDEX idx_cl_#year_quarter_id_mcaid_cond_id 
ON #year_quarter_id_mcaid_cond_id([id_mcaid], [cond_id], [year_quarter]);

/*
Step 3:

First, determine if each cond_id is present a 4-quarter window.
This is done by MAX([elixhauser_wgt]) within each 4-quarter window.

Second, SUM([elixhauser_wgt]) GROUP BY [year_month], [id_mcaid]
to get each weighted score within a 4-quarter window.
*/

WITH [max_over_window] AS
(
SELECT
 [year_quarter]
,[id_mcaid]
,[cond_id]
--,[elixhauser_wgt]
--,[charlson_wgt]
--,[gagne_wgt]
,MAX([elixhauser_wgt]) OVER(PARTITION BY [id_mcaid], [cond_id] ORDER BY [year_quarter] ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS [elixhauser_wgt]
,MAX([charlson_wgt]) OVER(PARTITION BY [id_mcaid], [cond_id] ORDER BY [year_quarter] ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS [charlson_wgt]
,MAX([gagne_wgt]) OVER(PARTITION BY [id_mcaid], [cond_id] ORDER BY [year_quarter] ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS [gagne_wgt]
--INTO #stage_mcaid_member_snapshot
FROM #year_quarter_id_mcaid_cond_id
),

[sum_over_cond_id] AS
(
SELECT 
 b.[year_month]
,a.[id_mcaid]
,SUM([elixhauser_wgt]) AS [elixhauser_t_12_m] 
,SUM([charlson_wgt]) AS [charlson_t_12_m] 
,SUM([gagne_wgt]) AS [gagne_t_12_m]
--INTO #stage_mcaid_member_snapshot
FROM [max_over_window] AS a
INNER JOIN #year_month AS b
ON a.[year_quarter] = b.[year_quarter]
GROUP BY
 b.[year_month]
,a.[id_mcaid]
)

INSERT INTO [stage].[mcaid_periodic_snapshot]

SELECT 
 b.[beg_measure_year_month] AS [beg_year_month]
,a.[year_month] AS [end_year_month]
,a.[id_mcaid]
,ISNULL([elixhauser_t_12_m], 0) AS [elixhauser_t_12_m] 
,ISNULL([charlson_t_12_m], 0) AS [charlson_t_12_m] 
,ISNULL([gagne_t_12_m], 0) AS [gagne_t_12_m]
--INTO #stage_mcaid_member_snapshot
FROM [sum_over_cond_id] AS a
INNER JOIN [ref].[perf_year_month] AS b
ON a.[year_month] = b.[year_month]
WHERE COALESCE([elixhauser_t_12_m], [charlson_t_12_m], [gagne_t_12_m]) IS NOT NULL
ORDER BY [id_mcaid], [end_year_month];

END
GO

/*
EXEC [stage].[sp_load_mcaid_periodic_snapshot];
*/

/*
SELECT
 [year_month]
,AVG(CAST([elixhauser_t_12_m] AS NUMERIC)) AS [elixhauser_t_12_m]
,AVG(CAST([charlson_t_12_m] AS NUMERIC)) AS [charlson_t_12_m]
,AVG(CAST([gagne_t_12_m] AS NUMERIC)) AS [gagne_t_12_m]
FROM [stage].[perf_enroll_denom] AS a
LEFT JOIN [stage].[mcaid_periodic_snapshot] AS b
ON a.[id_mcaid] = b.[id_mcaid]
AND a.[year_month] = b.[end_year_month]
WHERE [end_quarter] = 1
AND [full_criteria_t_12_m] >= 11
AND [end_month_age] >= 18
GROUP BY [year_month]
ORDER BY [year_month];
*/