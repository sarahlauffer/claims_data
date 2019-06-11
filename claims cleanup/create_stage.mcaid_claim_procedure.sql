
/*
This code creates table ([stage].[mcaid_claim_procedure]) to hold DISTINCT 
procedure codes in long format for Medicaid claims data

SQL script created by: Eli Kern, APDE, PHSKC, 2018-03-21
R functions created by: Alastair Matheson, PHSKC (APDE), 2019-05
Modified by: Philip Sylling, 2019-06-11

Data Pull Run time: 00:12:11
Create Index Run Time: 00:05:32

Returns
[stage].[mcaid_claim_procedure]
 [id_mcaid]
,[claim_header_id]
,[pcode]
,[proc_number]
,[pcode_mod_1]
,[pcode_mod_2]
,[pcode_mod_3]
,[pcode_mod_4]
*/

use PHClaims;
go

--if object_id('[stage].[mcaid_claim_procedure]', 'U') IS NOT NULL
--drop table [stage].[mcaid_claim_procedure];
if object_id('tempdb..#mcaid_claim_procedure', 'U') IS NOT NULL 
drop table #mcaid_claim_procedure;

select distinct 
 cast(id_mcaid as varchar(200)) as id_mcaid
,cast(claim_header_id as bigint) as claim_header_id
,cast(pcode as varchar(200)) as pcode
--,cast(substring(proc_number, 6,4) as varchar(4)) as 'proc_number',
,cast(proc_number as varchar(2)) as proc_number
,cast(MDFR_CODE1 as varchar(200)) as pcode_mod_1
,cast(MDFR_CODE2 as varchar(200)) as pcode_mod_2
,cast(MDFR_CODE3 as varchar(200)) as pcode_mod_3
,cast(MDFR_CODE4 as varchar(200)) as pcode_mod_4

--into [stage].[mcaid_claim_procedure]
into #mcaid_claim_procedure

from 
(
select 
--top(100)
 MEDICAID_RECIPIENT_ID AS id_mcaid
,TCN as claim_header_id
,PRCDR_CODE_1 AS [01]
,PRCDR_CODE_2 AS [02]
,PRCDR_CODE_3 AS [03]
,PRCDR_CODE_4 AS [04]
,PRCDR_CODE_5 AS [05]
,PRCDR_CODE_6 AS [06]
,PRCDR_CODE_7 AS [07]
,PRCDR_CODE_8 AS [08]
,PRCDR_CODE_9 AS [09]
,PRCDR_CODE_10 AS [10]
,PRCDR_CODE_11 AS [11]
,PRCDR_CODE_12 AS [12]
,LINE_PRCDR_CODE as [line]
,MDFR_CODE1
,MDFR_CODE2
,MDFR_CODE3
,MDFR_CODE4
from [stage].[mcaid_claim]
) AS a

unpivot(pcode for proc_number IN ([01],[02],[03],[04],[05],[06],[07],[08],[09],[10],[11],[12],[line])) as pcode

/*
--create indexes
CREATE CLUSTERED INDEX [idx_cl_stage_mcaid_claim_procedure_claim_header_id] 
ON [stage].[mcaid_claim_procedure]([claim_header_id]);
GO
CREATE NONCLUSTERED INDEX [idx_nc_stage_mcaid_claim_procedure_pcode] 
ON [stage].[mcaid_claim_procedure]([pcode]);
GO

CREATE CLUSTERED INDEX [idx_cl_#mcaid_claim_procedure_claim_header_id] 
ON #mcaid_claim_procedure([claim_header_id]);
GO
CREATE NONCLUSTERED INDEX [idx_nc_#mcaid_claim_procedure_pcode] 
ON #mcaid_claim_procedure([pcode]);
GO
*/

