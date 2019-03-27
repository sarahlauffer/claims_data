
USE PHClaims;
GO

/*
IF OBJECT_ID('[ref].[hedis_code_system]') IS NOT NULL
DROP TABLE [ref].[hedis_code_system];
SELECT *
INTO [ref].[hedis_code_system]
FROM [dbo].[ref_hedis_code_system];

ALTER TABLE [ref].[hedis_code_system] ADD CONSTRAINT [PK_ref_hedis_code_system] PRIMARY KEY CLUSTERED 
([value_set_name]
,[code_system]
,[code]);
GO

IF OBJECT_ID('[ref].[hedis_measure]') IS NOT NULL
DROP TABLE [ref].[hedis_measure];
SELECT *
INTO [ref].[hedis_measure]
FROM [dbo].[ref_hedis_measure];

ALTER TABLE [ref].[hedis_measure] ADD CONSTRAINT [PK_ref_hedis_measure] PRIMARY KEY CLUSTERED 
([measure_id]);
GO

IF OBJECT_ID('[ref].[hedis_medication_list]') IS NOT NULL
DROP TABLE [ref].[hedis_medication_list];
SELECT *
INTO [ref].[hedis_medication_list]
FROM [dbo].[ref_hedis_medication_list];

ALTER TABLE [ref].[hedis_medication_list] ADD CONSTRAINT [PK_ref_hedis_medication_list] PRIMARY KEY CLUSTERED 
([measure_id]
,[medication_list_name]);
GO

IF OBJECT_ID('[ref].[hedis_ndc_codes]') IS NOT NULL
DROP TABLE [ref].[hedis_ndc_codes];
SELECT *
INTO [ref].[hedis_ndc_codes]
FROM [dbo].[ref_hedis_ndc_codes];

ALTER TABLE [ref].[hedis_ndc_codes] ADD CONSTRAINT [PK_ref_hedis_ndc_codes] PRIMARY KEY CLUSTERED 
([medication_list_name]
,[ndc_code]);
GO

IF OBJECT_ID('[ref].[hedis_value_set]') IS NOT NULL
DROP TABLE [ref].[hedis_value_set];
SELECT *
INTO [ref].[hedis_value_set]
FROM [dbo].[ref_hedis_value_set];

ALTER TABLE [ref].[hedis_value_set] ADD CONSTRAINT [PK_ref_hedis_value_set] PRIMARY KEY CLUSTERED 
([measure_id]
,[value_set_name]);
GO
*/

/*
IF EXISTS (SELECT 1 FROM sys.objects WHERE NAME = 'FK_ref_hedis_code_system_ref_hedis_value_set')
ALTER TABLE [ref].[hedis_code_system] DROP CONSTRAINT [FK_ref_hedis_code_system_ref_hedis_value_set];
IF EXISTS (SELECT 1 FROM sys.objects WHERE NAME = 'FK_ref_hedis_value_set_ref_hedis_measure')
ALTER TABLE [ref].[hedis_value_set] DROP CONSTRAINT [FK_ref_hedis_value_set_ref_hedis_measure];

IF EXISTS (SELECT 1 FROM sys.objects WHERE NAME = 'PK_ref_hedis_code_system')
ALTER TABLE [ref].[hedis_code_system] DROP CONSTRAINT [PK_ref_hedis_code_system];
IF EXISTS (SELECT 1 FROM sys.objects WHERE NAME = 'PK_ref_hedis_value_set')
ALTER TABLE [ref].[hedis_value_set] DROP CONSTRAINT [PK_ref_hedis_value_set];
IF EXISTS (SELECT 1 FROM sys.objects WHERE NAME = 'PK_ref_hedis_measure')
ALTER TABLE [ref].[hedis_measure] DROP CONSTRAINT [PK_ref_hedis_measure];
*/

IF OBJECT_ID('[ref].[hedis_measure]', 'U') IS NOT NULL
DROP TABLE [ref].[hedis_measure];
CREATE TABLE [ref].[hedis_measure]
([measure_id] VARCHAR(5) NOT NULL
,[measure_name] VARCHAR(200) NOT NULL
,CONSTRAINT [PK_ref_hedis_measure] PRIMARY KEY CLUSTERED([measure_id])
);
GO

TRUNCATE TABLE [ref].[hedis_measure];
INSERT INTO [ref].[hedis_measure]
([measure_id]
,[measure_name])
SELECT DISTINCT 
 CASE WHEN [Measure.ID] = 'GG' AND [Measure.Name] = 'Identifying Events/Diagnoses Using Laboratory or Pharmacy Data' THEN 'GG_1'
      WHEN [Measure.ID] = 'GG' AND [Measure.Name] = 'Members in Hospice' THEN 'GG_2'
	  ELSE [Measure.ID]
 END AS [measure_id]
,[Measure.Name] AS [measure_name]
FROM [KC\psylling].[tmp_2018 Volume 2 Value Set Directory 07_03_2017-2.xlsx];

INSERT INTO [ref].[hedis_measure]
([measure_id]
,[measure_name])
SELECT DISTINCT
 [Measure.ID] AS [measure_id]
,[Measure.Name] AS [measure_name]
FROM [KC\psylling].[tmp_20180208_HEDIS_NDC_MLD_CompleteDirectory_Workbook_2018-2.xlsx]
WHERE [Measure.ID] NOT IN
(
SELECT DISTINCT [measure_id] FROM [ref].[hedis_measure]
);

IF OBJECT_ID('[ref].[hedis_value_set]', 'U') IS NOT NULL
DROP TABLE [ref].[hedis_value_set];
CREATE TABLE [ref].[hedis_value_set]
([measure_id] VARCHAR(5) NOT NULL
,[value_set_name] VARCHAR(100) NOT NULL
,[value_set_oid] VARCHAR(50) NOT NULL
,CONSTRAINT [PK_ref_hedis_value_set] PRIMARY KEY CLUSTERED([measure_id], [value_set_name])
);
GO

TRUNCATE TABLE [ref].[hedis_value_set];
INSERT INTO [ref].[hedis_value_set]
([measure_id]
,[value_set_name]
,[value_set_oid])
SELECT 
 CASE WHEN [Measure.ID] = 'GG' AND [Measure.Name] = 'Identifying Events/Diagnoses Using Laboratory or Pharmacy Data' THEN 'GG_1'
      WHEN [Measure.ID] = 'GG' AND [Measure.Name] = 'Members in Hospice' THEN 'GG_2'
	  ELSE [Measure.ID]
 END AS [measure_id]
,[Value.Set.Name] AS [value_set_name]
-- There is an error in the HIV value set oid on the second tab of the hedis workbook - this corrects
,CASE WHEN [Value.Set.Name] = 'HIV' AND [Value.Set.OID] = '2.16.840.1.113883.3.464.1004.1406'
      THEN '2.16.840.1.113883.3.464.1004.1110'
	  ELSE [Value.Set.OID]
 END AS [value_set_oid]
FROM [KC\psylling].[tmp_2018 Volume 2 Value Set Directory 07_03_2017-2.xlsx];

IF OBJECT_ID('[ref].[hedis_medication_list]', 'U') IS NOT NULL
DROP TABLE [ref].[hedis_medication_list];
CREATE TABLE [ref].[hedis_medication_list]
([measure_id] VARCHAR(5) NOT NULL
,[medication_list_name] VARCHAR(100) NOT NULL
,CONSTRAINT [PK_ref_hedis_medication_list] PRIMARY KEY CLUSTERED([measure_id], [medication_list_name])
);
GO

TRUNCATE TABLE [ref].[hedis_medication_list];
INSERT INTO [ref].[hedis_medication_list]
([measure_id]
,[medication_list_name])
SELECT 
 [Measure.ID] AS [measure_id]
,[Medication.List.Name] AS [medication_list_name]
FROM [KC\psylling].[tmp_20180208_HEDIS_NDC_MLD_CompleteDirectory_Workbook_2018-2.xlsx];

IF OBJECT_ID('[ref].[hedis_code_system]', 'U') IS NOT NULL
DROP TABLE [ref].[hedis_code_system];
CREATE TABLE [ref].[hedis_code_system]
([value_set_name] VARCHAR(100) NOT NULL
,[code_system] VARCHAR(50) NOT NULL
,[code] VARCHAR(50) NOT NULL
,[definition] VARCHAR(2000)
,[value_set_version] DATE NOT NULL
,[code_system_version] VARCHAR(50) NOT NULL
,[value_set_oid] VARCHAR(50) NOT NULL
,[code_system_oid] VARCHAR(50)
,CONSTRAINT [PK_ref_hedis_code_system] PRIMARY KEY CLUSTERED([value_set_name], [code_system], [code])
);
GO

TRUNCATE TABLE [ref].[hedis_code_system];
INSERT INTO [ref].[hedis_code_system]
([value_set_name]
,[code_system]
,[code]
,[definition]
,[value_set_version]
,[code_system_version]
,[value_set_oid]
,[code_system_oid])

SELECT 
 [Value.Set.Name] AS [value_set_name]
,CASE WHEN [Code.System] = 'CPT' AND [Value.Set.Name] LIKE '%Modifier%' THEN 'CPT Modifier' ELSE [Code.System] END AS [code_system]
,CASE WHEN [Code.System] = 'ICD10CM' THEN REPLACE([Code], '.', '') 
      WHEN [Code.System] = 'ICD9CM' THEN REPLACE(CAST(REPLACE([Code], '.', '') AS CHAR(5)), ' ', '0')
	  WHEN [Code.System] = 'ICD9PCS' THEN REPLACE([Code], '.', '')
	  WHEN [Code.System] = 'UBTOB' THEN SUBSTRING([Code], 2, 3)
	  ELSE [Code] 
 END AS [code]
,[Definition] AS [definition]
,[Value.Set.Version] AS [value_set_version]
,[Code.System.Version] AS [code_system_version]
,[Value.Set.OID] AS [value_set_oid]
,[Code.System.OID] AS [code_system_oid]
FROM [KC\psylling].[tmp_2018 Volume 2 Value Set Directory 07_03_2017-3.xlsx];

IF OBJECT_ID('[ref].[hedis_ndc_codes]', 'U') IS NOT NULL
DROP TABLE [ref].[hedis_ndc_codes];
CREATE TABLE [ref].[hedis_ndc_codes]
([medication_list_name] VARCHAR(100) NOT NULL
,[ndc_code] VARCHAR(20) NOT NULL
,[brand_name] VARCHAR(100)
,[generic_product_name] VARCHAR(200)
,[route] VARCHAR(50)
,[description] VARCHAR(200)
,[drug_id] VARCHAR(20)
,[drug_name] VARCHAR(50)
,[package_size] NUMERIC(18,4)
,[unit] VARCHAR(20)
,[dose] NUMERIC(18,4)
,[form] VARCHAR(20)
,[med_conversion_factor] NUMERIC(18,4)
,CONSTRAINT [PK_ref_hedis_ndc_codes] PRIMARY KEY CLUSTERED([medication_list_name], [ndc_code])
);
GO

TRUNCATE TABLE [ref].[hedis_ndc_codes]
INSERT INTO [ref].[hedis_ndc_codes]
([medication_list_name]
,[ndc_code]
,[brand_name]
,[generic_product_name]
,[route]
,[description]
,[drug_id]
,[drug_name]
,[package_size]
,[unit]
,[dose]
,[form]
,[med_conversion_factor])

-- There are 10 duplicated rows in the HEDIS spreadsheet (71,843 rows)
SELECT DISTINCT
 CASE WHEN [Medication.List] = 'N/A' THEN NULL ELSE [Medication.List] END AS [medication_list_name]
,CASE WHEN [NDC.Code] = 'N/A' THEN NULL ELSE [NDC.Code] END AS [ndc_code]
,CASE WHEN [Brand.Name] = 'N/A' THEN NULL ELSE [Brand.Name] END AS [brand_name]
,CASE WHEN [Generic.Product.Name] = 'N/A' THEN NULL ELSE [Generic.Product.Name] END AS [generic_product_name] 
,CASE WHEN [Route] = 'N/A' THEN NULL ELSE [Route] END AS [route]
,CASE WHEN [Description] = 'N/A' THEN NULL ELSE [Description] END AS [description] 
,CASE WHEN [Drug.ID] = 'N/A' THEN NULL ELSE [Drug.ID] END AS [drug_id]
,CASE WHEN [Drug.Name] = 'N/A' THEN NULL ELSE [Drug.Name] END AS [drug_name]
,CASE WHEN [Package.Size] = 'N/A' THEN NULL ELSE CAST([Package.Size] AS NUMERIC(18, 4)) END AS [package_size]
,CASE WHEN [Unit] = 'N/A' THEN NULL ELSE [Unit] END AS [unit]
,CASE WHEN [Dose] = 'N/A' THEN NULL ELSE CAST([Dose] AS NUMERIC(18, 4)) END AS [dose]
,CASE WHEN [Form] = 'N/A' THEN NULL ELSE [Form] END AS [form]
,CASE WHEN [MED.Conversion.Factor] = 'N/A' THEN NULL ELSE CAST([MED.Conversion.Factor] AS NUMERIC(18, 4)) END AS [med_conversion_factor]
FROM [KC\psylling].[tmp_20180208_HEDIS_NDC_MLD_CompleteDirectory_Workbook_2018-3.xlsx];