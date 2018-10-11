--Code to create table to hold DISTINCT header-level claim information -> dbo.mcaid_claim_summary
--Eli Kern
--APDE, PHSKC
--6/6/2018

--6/6/18 update: added RDA-based MH and SUD flags
--7/26/18 update: added NY ED classification algorithm
--9/6/18 update: reran table to include new ICD-CCS mappings based on request from Titus
--10/5/18 update: add primary diagnosis and version to table, add SDOH flag for ICD-10-CM codes

--Run time: approximately 36 min, over 50 million records

--------------------------------------
--STEP 1: create temp summary claims table with event-based flags
--------------------------------------
if object_id('tempdb..#mcaid_claim_summary_step1') IS NOT NULL 
  drop table #mcaid_claim_summary_step1

select header.id, header.tcn, header.clm_type_code, header.from_date, header.to_date,
	header.patient_status, header.adm_source, header.pos_code, header.clm_cat_code,
	header.bill_type_code, header.clm_status_code, header.billing_npi,
	header.drg_code, header.unit_srvc_h,

	--Mental health-related primary diagnosis
	case when header.mh_drg = 1 or diag.dx1_mental = 1
	then 1 else 0 end as 'mental_dx1',

	--Mental health-related, any diagnosis
	case when header.mh_drg = 1 or diag.dxany_mental = 1
	then 1 else 0 end as 'mental_dxany',

	--Maternity-related care (primary diagnosis only)
	case when header.maternal_drg_tob = 1 or line.maternal_rcode = 1 or diag.dx1_maternal = 1
	then 1 else 0 end as 'maternal_dx1',

	--Maternity-related care (primary diagnosis only), broader definition for diagnosis codes
	case when header.maternal_drg_tob = 1 or line.maternal_rcode = 1 or diag.dx1_maternal_broad = 1
	then 1 else 0 end as 'maternal_broad_dx1',

	--Newborn-related care (prim. diagnosis only)
	case when header.newborn_drg = 1 or diag.dx1_newborn = 1
	then 1 else 0 end as 'newborn_dx1',

	--Inpatient stay flag
	header.inpatient,

	--ED visit (broad definition)
	case when header.clm_type_code in (3,26,34)
		and (line.ed_rcode = 1
		or pcode.ed_pcode1 = 1
		or (pos_code = '23' and ed_pcode2 = 1))
	then 1 else 0 end as 'ed',

	--Primary diagnosis and version
	diag.dx_norm, diag.dx_ver,

	--SDOH flags
	diag.sdoh_any

into #mcaid_claim_summary_step1

--select header-level information needed for event flags		
from (
	select *,

	--inpatient stay
	case when clm_type_code in (31,33) then 1 else 0 end as 'inpatient',

	--mental health-related DRG
	case when drg_code between '876' and '897'
		or drg_code between '945' and '946'
	then 1 else 0 end as 'mh_drg',

	--newborn/liveborn infant-related DRG
	case when drg_code between '789' and '795'
	then 1 else 0 end as 'newborn_drg',

	--maternity-related DRG or type of bill
	case when bill_type_code in ('840','841','842','843','844','845','847','848','84F','84G','84H','84I','84J','84K','84M','84O','84X','84Y','84Z')
		or drg_code between '765' and '782'
	then 1 else 0 end as 'maternal_drg_tob'

	from PHClaims.dbo.mcaid_claim_header
) header

--select diagnosis code information needed for event flags
left join (
	select tcn,

	--primary diagnosis code with version
	max(case when dx_number = 1 then dx_norm else null end) as dx_norm,
	max(case when dx_number = 1 then dx_ver else null end) as dx_ver,
	
	--mental health-related primary diagnosis (HEDIS 2017)
	max(case when dx_number = 1
		and ((dx_norm between '290' and '316' and dx_ver = 9 )
		or (dx_norm between 'F03' and 'F0391' and dx_ver = 10)
		or (dx_norm between 'F10' and 'F69' and dx_ver = 10)
		or (dx_norm between 'F80' and 'F99' and dx_ver = 10))
	then 1 else 0 end) as 'dx1_mental',

	--mental health-related, any diagnosis (HEDIS 2017)
	max(case when ((dx_norm between '290' and '316' and dx_ver = 9)
		or (dx_norm between 'F03' and 'F0391' and dx_ver = 10)
		or (dx_norm between 'F10' and 'F69' and dx_ver = 10)
		or (dx_norm between 'F80' and 'F99' and dx_ver = 10))
	then 1 else 0 end) as 'dxany_mental',

	--newborn-related primary diagnosis (HEDIS 2017)
	max(case when dx_number = 1
		and ((dx_norm between 'V30' and 'V39' and dx_ver = 9)
		or (dx_norm between 'Z38' and 'Z389' and dx_ver = 10))
	then 1 else 0 end) as 'dx1_newborn',

	--maternity-related primary diagnosis (HEDIS 2017)
	max(case when dx_number = 1
		and  ((dx_norm between '630' and '679' and dx_ver = 9)
		or (dx_norm between 'V24' and 'V242' and dx_ver = 9)
		or (dx_norm between 'O00' and 'O9279' and dx_ver = 10)
		or (dx_norm between 'O98' and 'O9989' and dx_ver = 10)
		or (dx_norm between 'O9A' and 'O9A53' and dx_ver = 10)
		or (dx_norm between 'Z0371' and 'Z0379' and dx_ver = 10)
		or (dx_norm between 'Z332' and 'Z3329' and dx_ver = 10)
		or (dx_norm between 'Z39' and 'Z3909' and dx_ver = 10))
	then 1 else 0 end) as 'dx1_maternal',

	--maternity-related primary diagnosis (broader)
	max(case when dx_number = 1
		and  ((dx_norm between '630' and '679' and dx_ver = 9)
		or (dx_norm between 'V20' and 'V29' and dx_ver = 9) /*broader*/
		or (dx_norm between 'O00' and 'O9279' and dx_ver = 10)
		or (dx_norm between 'O94' and 'O9989' and dx_ver = 10) /*broader*/
		or (dx_norm between 'O9A' and 'O9A53' and dx_ver = 10)
		or (dx_norm between 'Z0371' and 'Z0379' and dx_ver = 10)
		or (dx_norm between 'Z30' and 'Z392' and dx_ver = 10) /*broader*/
		or (dx_norm between 'Z3A0' and 'Z3A49' and dx_ver = 10)) /*broader*/
	then 1 else 0 end) as 'dx1_maternal_broad',

	--SDOH-related (any diagnosis)
	max(case when dx_norm between 'Z55' and 'Z659' and dx_ver = 10
	then 1 else 0 end) as 'sdoh_any'

	from PHClaims.dbo.mcaid_claim_dx
	group by tcn
) diag
on header.tcn = diag.tcn

--select procedure code information needed for event flags
left join (
	select tcn,
	
	--ed visits sub-flags
	max(case when pcode like '9928[123458]'
	then 1 else 0 end) as 'ed_pcode1',
	max(case when pcode between '10021' and '69990'
	then 1 else 0 end) as 'ed_pcode2'

	from PHClaims.dbo.mcaid_claim_proc
	group by tcn
) pcode
on header.tcn = pcode.tcn

--select line-level information needed for event flags
left join (
	select tcn, 

	--ed visits sub-flags
	max(case when rcode like '045[01269]' or rcode like '0981'
	then 1 else 0 end) as 'ed_rcode',

	--maternity revenue codes
	max(case when rcode in ('0112','0122','0132','0142','0152','0720','0721','0722','0724')
	then 1 else 0 end) as 'maternal_rcode'

	from PHClaims.dbo.mcaid_claim_line
	group by tcn
) line
on header.tcn = line.tcn

--------------------------------------
--STEP 2: create flags that require comparison of previously created event-based flags across time
--------------------------------------
if object_id('tempdb..#mcaid_claim_summary_step2') IS NOT NULL 
  drop table #mcaid_claim_summary_step2

select a.*, case when ed_nohosp.ed_nohosp = 1 then 1 else 0 end as 'ed_nohosp'

into #mcaid_claim_summary_step2

--ED visit flags
from (select * from #mcaid_claim_summary_step1) as a
left join (
	select y.id, y.tcn, ed_nohosp = 1
	from (
		--group by ID and ED visit date and take minimum difference to get closest inpatient stay
		select distinct x.id, x.tcn, min(x.eh_ddiff) as 'eh_ddiff_pmin'
		from (
			select distinct e.id, ed_date = e.from_date, hosp_date = h.from_date, tcn,
				--create field that calculates difference in days between each ED visit and following inpatient stay
				--set to null when comparison is between ED visits and PRIOR inpatient stays
				case
					when datediff(dd, e.from_date, h.from_date) >=0 then datediff(dd, e.from_date, h.from_date)
					else null
				end as 'eh_ddiff'
			from #mcaid_claim_summary_step1 as e
			left join (
				select distinct id, from_date
				from #mcaid_claim_summary_step1
				where inpatient = 1
			) as h
			on e.id = h.id
			where e.ed = 1
		) as x
		group by x.id, x.tcn
	) as y
	where y.eh_ddiff_pmin > 1 or y.eh_ddiff_pmin is null
) ed_nohosp
on a.tcn = ed_nohosp.tcn

--------------------------------------
--STEP 3: create final summary claims table with all event-based flags
--------------------------------------
use PHClaims
go

if object_id('dbo.mcaid_claim_summary_load', 'U') IS NOT NULL 
  drop table dbo.mcaid_claim_summary_load;

select a.*,
	
	--ED-related flags
	case when a.ed = 1 and a.mental_dxany = 1 then 1 else 0 end as 'ed_bh',
	case when a.ed = 1 and b.ed_avoid_ca = 1 then 1 else 0 end as 'ed_avoid_ca',
	case when a.ed_nohosp = 1 and b.ed_avoid_ca = 1 then 1 else 0 end as 'ed_avoid_ca_nohosp',
	case when a.ed = 1 and (f.ed_needed_unavoid_nyu + f.ed_needed_avoid_nyu) > 0.50 then 1 else 0 end as 'ed_emergent_nyu',
	case when a.ed = 1 and (f.ed_pc_treatable_nyu + f.ed_nonemergent_nyu) > 0.50 then 1 else 0 end as 'ed_nonemergent_nyu',
	case when a.ed = 1 and (((f.ed_needed_unavoid_nyu + f.ed_needed_avoid_nyu) = 0.50) or 
			((f.ed_pc_treatable_nyu + f.ed_nonemergent_nyu) = 0.50)) then 1 else 0 end as 'ed_intermediate_nyu',
	case when a.ed = 1 and f.ed_mh_nyu > 0.50 then 1 else 0 end as 'ed_mh_nyu',
	case when a.ed = 1 and f.ed_sud_nyu > 0.50 then 1 else 0 end as 'ed_sud_nyu',
	case when a.ed = 1 and f.ed_alc_nyu > 0.50 then 1 else 0 end as 'ed_alc_nyu',
	case when a.ed = 1 and f.ed_injury_nyu > 0.50 then 1 else 0 end as 'ed_injury_nyu',
	case when a.ed = 1 and f.ed_unclass_nyu > 0.50 then 1 else 0 end as 'ed_unclass_nyu',

	--Inpatient-related flags
	case when a.inpatient = 1 and a.mental_dx1 = 0 and a.newborn_dx1 = 0 and a.maternal_dx1 = 0 then 1 else 0 end as 'ipt_medsurg',
	case when a.inpatient = 1 and a.mental_dxany = 1 then 1 else 0 end as 'ipt_bh',
	
	--Injuries
	c.intent, c.mechanism,

	--CCS
	d.ccs, d.ccs_description, d.multiccs_lv1 as 'ccs_mult1', d.multiccs_lv1_description as 'ccs_mult1_description', d.multiccs_lv2 as 'ccs_mult2', 
	d.multiccs_lv2_description as 'ccs_mult2_description',
	
	--RDA MH and SUD flagas
	case when e.mental_dx_rda_any = 1 then 1 else 0 end as 'mental_dx_rda_any', 
	case when e.sud_dx_rda_any = 1 then 1 else 0 end as 'sud_dx_rda_any',

	--SDOH ED and IPT flags
	case when a.ed = 1 and a.sdoh_any = 1 then 1 else 0 end as 'ed_sdoh',
	case when a.inpatient = 1 and a.sdoh_any = 1 then 1 else 0 end as 'ipt_sdoh'

into PHClaims.dbo.mcaid_claim_summary_load

from (select * from #mcaid_claim_summary_step2) as a

--Avoidable ED visit flags, California algorithm
left join (
	select diag.tcn, max(avoid.ed_avoid_ca) as 'ed_avoid_ca'

	from (
	select dx, dx_ver, ed_avoid_ca
	from PHClaims.dbo.ref_dx_lookup
	where ed_avoid_ca = 1
	) as avoid

	inner join (
	SELECT tcn, dx_norm, dx_ver
	  FROM [PHClaims].[dbo].[mcaid_claim_dx]
	  where dx_number = 1
	) as diag
	on (avoid.dx = diag.dx_norm) and (avoid.dx_ver = diag.dx_ver)
	group by diag.tcn
) as b
on a.tcn = b.tcn

--Injury intent and mechanism, primary diagnosis
left join (
	select diag.tcn, injury.intent, injury.mechanism

	from (
	select dx, dx_ver, intent, mechanism
	from PHClaims.dbo.ref_dx_lookup
	where intent is not null
	) as injury

	inner join (
	SELECT tcn, dx_norm, dx_ver
	  FROM [PHClaims].[dbo].[mcaid_claim_dx]
	  where dx_number = 1
	) as diag
	on (injury.dx = diag.dx_norm) and (injury.dx_ver = diag.dx_ver)
) as c
on a.tcn = c.tcn

--CCS groupings (CCS, CCS-level 1, CCS-level 2), primary diagnosis
left join (
	select diag.tcn, ccs.ccs, ccs.ccs_description, ccs.multiccs_lv1, ccs.multiccs_lv1_description, ccs.multiccs_lv2, ccs.multiccs_lv2_description

	from (
	select dx, dx_ver, ccs, ccs_description, multiccs_lv1, multiccs_lv1_description, multiccs_lv2, multiccs_lv2_description
	from PHClaims.dbo.ref_dx_lookup
	) as ccs

	inner join (
	SELECT tcn, dx_norm, dx_ver
	  FROM [PHClaims].[dbo].[mcaid_claim_dx]
	  where dx_number = 1
	) as diag
	on (ccs.dx = diag.dx_norm) and (ccs.dx_ver = diag.dx_ver)
) as d
on a.tcn = d.tcn

--RDA Mental health and Substance use disorder diagnosis flags, any diagnosis
left join (
	select rda_diag.tcn, max(rda_diag.mental_dx_rda) as 'mental_dx_rda_any', max(rda_diag.sud_dx_rda) as 'sud_dx_rda_any'
	from (
		select diag.tcn, rda.mental_dx_rda, rda.sud_dx_rda

		from (
		select dx, dx_ver, mental_dx_rda, sud_dx_rda
		from PHClaims.dbo.ref_dx_lookup
		) as rda

		inner join (
		SELECT tcn, dx_norm, dx_ver
		  FROM [PHClaims].[dbo].[mcaid_claim_dx]
		) as diag
		on (rda.dx = diag.dx_norm) and (rda.dx_ver = diag.dx_ver)
	) as rda_diag
	group by rda_diag.tcn
) as e
on a.tcn = e.tcn

--ED visit classification, NYU algorithm
left join (
	select diag.tcn, avoid_nyu.ed_needed_unavoid_nyu, avoid_nyu.ed_needed_avoid_nyu,
		avoid_nyu.ed_pc_treatable_nyu, avoid_nyu.ed_nonemergent_nyu, avoid_nyu.ed_mh_nyu, 
		avoid_nyu.ed_sud_nyu, avoid_nyu.ed_alc_nyu, avoid_nyu.ed_injury_nyu,
		avoid_nyu.ed_unclass_nyu
	
	from (
	select dx, dx_ver, ed_needed_unavoid_nyu, ed_needed_avoid_nyu,
		ed_pc_treatable_nyu, ed_nonemergent_nyu, ed_mh_nyu, ed_sud_nyu,
		ed_alc_nyu, ed_injury_nyu, ed_unclass_nyu
	from PHClaims.dbo.ref_dx_lookup
	) as avoid_nyu

	inner join (
	SELECT tcn, dx_norm, dx_ver
	  FROM [PHClaims].[dbo].[mcaid_claim_dx]
	  where dx_number = 1
	) as diag
	on (avoid_nyu.dx = diag.dx_norm) and (avoid_nyu.dx_ver = diag.dx_ver)
) as f
on a.tcn = f.tcn