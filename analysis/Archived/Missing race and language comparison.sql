--total IDs
select count(distinct x.MEDICAID_RECIPIENT_ID) as 'id_cnt'
from(
SELECT distinct CLNDR_YEAR_MNTH, [MEDICAID_RECIPIENT_ID]
		,[RACE1]
		,[RACE2]
		,[RACE3]
		,[RACE4]
		,[HISPANIC_ORIGIN_NAME]
	FROM [PHClaims].[dbo].[NewEligibility]
	where CLNDR_YEAR_MNTH >= '201701' and CLNDR_YEAR_MNTH <= '201706'
) as x

--IDs with missing, other, not provided race, and missing hispanic eth or not-hispanic in context of missing race
select count(distinct x.MEDICAID_RECIPIENT_ID) as 'id_cnt'
from(
SELECT distinct CLNDR_YEAR_MNTH, [MEDICAID_RECIPIENT_ID]
		,[RACE1]
		,[RACE2]
		,[RACE3]
		,[RACE4]
		,[HISPANIC_ORIGIN_NAME]
	FROM [PHClaims].[dbo].[NewEligibility]
	where CLNDR_YEAR_MNTH >= '201701' and CLNDR_YEAR_MNTH <= '201706' and 
	(RACE1 is null or RACE1 in ('Not Provided', 'Other')) and (RACE2 is null or RACE2 in ('Not Provided', 'Other')) and (RACE3 is null or RACE3 in ('Not Provided', 'Other')) and 
	(RACE4 is null or RACE4 in ('Not Provided', 'Other')) and (HISPANIC_ORIGIN_NAME is null or HISPANIC_ORIGIN_NAME in ('NOT HISPANIC'))
) as x

--IDs with null, undetermined or other language in both spoken and language fields
select count(distinct x.MEDICAID_RECIPIENT_ID) as 'id_cnt'
from(
SELECT distinct CLNDR_YEAR_MNTH, [MEDICAID_RECIPIENT_ID]
		,SPOKEN_LNG_NAME, WRTN_LNG_NAME
	FROM [PHClaims].[dbo].[NewEligibility]
	where CLNDR_YEAR_MNTH >= '201701' and CLNDR_YEAR_MNTH <= '201706' and 
	(SPOKEN_LNG_NAME is null or SPOKEN_LNG_NAME in ('Undetermined', 'Other Language')) and 
	(WRTN_LNG_NAME is null or WRTN_LNG_NAME in ('Undetermined', 'Other Language'))
) as x
