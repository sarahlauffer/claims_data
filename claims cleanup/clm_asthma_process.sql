--Code to create table to hold asthma diagnosis status (CCW) for collapsed time periods -> dbo.mcaid_claim_asthma
--Eli Kern
--APDE, PHSKC
--5/7/2018

/*
step 1: create a temp table holding ALL asthma claims and service date
step 2: create a person X rolling time period matrix
step 3: use the rolling time matrix and asthma claims to generate asthma CCW status for each 12-month period,
	and then collapse to contiguous time by person
*/

--Time for entire script to run: 40 seconds

--------------
--step 1: create temp table to hold asthma claims and dates
--------------

if object_id('tempdb..#asthma_tmp') IS NOT NULL 
drop table #asthma_tmp

select header.id, header.tcn, header.clm_type_code, header.from_date, diag.asthma_ccw, 

	--apply CCW claim type criteria to define asthma 1 (inpatient/SNF/HHA) and asthma 2 (outpatient, professional)
	case when header.clm_type_code in (31,33,12,23) then 1 else 0 end as 'asthma_1',
	case when header.clm_type_code in (1,3,26,27,28,34) then 1 else 0 end as 'asthma_2',
	case when header.clm_type_code in (31,33,12,23) then header.from_date else null end as 'asthma_1_from_date',
	case when header.clm_type_code in (1,3,26,27,28,34) then header.from_date else null end as 'asthma_2_from_date'

into #asthma_tmp

--pull out claim type and service dates
from (
	select id, tcn, clm_type_code, from_date
	from PHClaims.dbo.mcaid_claim_header
) header

--right join to claims containing a diagnosis in the CCW asthma definition
right join (
	select diag.id, diag.tcn, ref.asthma_ccw

	--pull out claim and diagnosis fields
	from (
		select id, tcn, dx_norm, dx_ver
		from PHClaims.dbo.mcaid_claim_dx
	) diag

	--join to diagnosis reference table, subset to those with asthma CCW
	inner join (
	select dx, dx_ver, asthma_ccw
	from PHClaims.dbo.ref_dx_lookup
	where asthma_ccw = 1
	) ref

	on (diag.dx_norm = ref.dx) and (diag.dx_ver = ref.dx_ver)
) as diag

on header.tcn = diag.tcn

--------------
--step 2: create temp table to hold ID and rolling time period matrix
--------------

if object_id('tempdb..#rolling12_tmp') IS NOT NULL 
drop table #rolling12_tmp

--join rolling time table to person ids
select id, start_window, end_window

into #rolling12_tmp

from (
	select distinct id, 'link' = 1 from #asthma_tmp
) as id

right join (
	select cast(start_window as date) as 'start_window', cast(end_window as date) as 'end_window',
		'link' = 1
	from dbo.ref_rolling_time_12mo_2012_2020
) as rolling

on id.link = rolling.link
order by id.id, rolling.start_window

--------------
--step 3: use asthma temp table and rolling time period matrix to identify asthma status over time and collapse to contiguous time periods
--------------

use PHClaims
go

if object_id('dbo.mcaid_claim_asthma_person', 'U') IS NOT NULL 
  drop table dbo.mcaid_claim_asthma_person;

--collapse to single row per ID and contiguous time period
select distinct d.id, min(d.start_window) as 'from_date', max(d.end_window) as 'to_date', 'asthma_ccw' = 1

into mcaid_claim_asthma_person

from (
	--set up groups where there is contiguous time
	select c.id, c.start_window, c.end_window, c.discont, c.temp_row,

		sum(case when c.discont is null then 0 else 1 end) over
			(order by c.id, c.temp_row rows between unbounded preceding and current row) as 'grp'

	from (
		--pull out ID and time periods that contain 1 asthma1 claim or 2 asthma2 claims at least 1 day apart
		select b.id, b.start_window, b.end_window, b.asthma_1_cnt, b.asthma_2_cnt, b.asthma_2_min_date, b.asthma_2_max_date,
			datediff(day, b.asthma_2_min_date, b.asthma_2_max_date) as 'asthma_2_datediff',

			--create a flag for a discontinuity in a person's disease status
			case
				when datediff(month, lag(b.start_window) over (partition by b.id order by b.id, b.start_window), b.start_window) <= 1 then null
				when b.start_window < lag(b.end_window) over (partition by b.id order by b.id, b.start_window) then null
				when row_number() over (partition by b.id order by b.id, b.start_window) = 1 then null
				else row_number() over (partition by b.id order by b.id, b.start_window)
			end as 'discont',

			row_number() over (partition by b.id order by b.id, b.start_window) as 'temp_row'

		from (
			--count asthma1 and asthma2 claims by ID and time period, and calculate minimum and maximum service date for each asthma2 claim by ID and time period
			select a.id, a.start_window, a.end_window,
				 sum(asthma_1) as 'asthma_1_cnt', sum(asthma_2) as 'asthma_2_cnt',
				 min(asthma_2_from_date) as 'asthma_2_min_date', max(asthma_2_from_date) as 'asthma_2_max_date'

			from (
				--pull ID, time period and asthma claim information, subset to ID x time period rows containing an asthma claim
				select matrix.id, matrix.start_window, matrix.end_window, ast.from_date, ast.asthma_1, ast.asthma_2, asthma_2_from_date

				--pull in ID x time period matrix
				from (
					select id, start_window, end_window
					from #rolling12_tmp
				) as matrix

				--join to asthma temp table
				left join (
					select id, from_date, asthma_1, asthma_2, asthma_2_from_date
					from #asthma_tmp
				) as ast

				on matrix.id = ast.id
				where ast.from_date between matrix.start_window and matrix.end_window
			) as a
			group by a.id, a.start_window, a.end_window
		) as b
		where (b.asthma_1_cnt >= 1) or (b.asthma_2_cnt >=2 and abs(datediff(day, b.asthma_2_min_date, b.asthma_2_max_date)) >=1)
	) as c
) as d
group by d.id, d.grp
order by d.id, from_date