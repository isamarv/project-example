/*
-----------------------------------------------------------------------------------------------------------------
-- Abax Healthcare, Inc
-- Contact: Jay Dalke
-- Modified: 03/05/2026
-- Email: support@abaxhealth.com
-- Please reach out to Jay Dalke for any assistance with this query. 
-- Query support is part of Abax Health's comprehensive implementation process.
-----------------------------------------------------------------------------------------------------------------
-- Purpose:
-- This query extracts detailed patient, order, and referral information for Abax Health.
-- It utilizes temporary tables and joins multiple data sources for comprehensive reporting.
-----------------------------------------------------------------------------------------------------------------
-- Filters:
-- @DATE FILTER
--		This filter is critical for capturing all referrals from the past year, ensuring no data is missed.
--		The filter retrieves referral records within the 365-day timeframe.
--		This ensures that Abax Health receives updated information on referrals captured within this period.
-- @DECEASED FILTER
--		The "Deceased" flag is designed to exclude deceased patients from system outputs, ensuring 
--		accurate and relevant data for results returned.
-- @ORDER CLASS FILTER
--		The filter removes records associated with order classes that are not in the scope of work.
--		Excluded Order Classes: (Adjust this list as needed)
--		'1'      -- Normal
--		'2'      -- Point of Care
--		'3'      -- Historical
--		'4'      -- Lab Collect
--		'6'      -- Over the Counter (OTC)
--		'9'      -- Phone In
--		'10'     -- No Print
--		'11'     -- Unit Collect
--		'12'     -- Print
--		'13'     -- No Print
--		'23'     -- Clinic Performed
--		'43'     -- Clinic Collect
-- @ORDER EXPIRED FILTER
--		The "Expired Order" filter is designed to exclude referrals with expired orders from system outputs, 
--		ensuring only active and actionable data is included in reports and workflows. The filter should be 
--		applied to all queries and integrations by excluding records where the order expiration date has passed.
-- @ORDER PROCEDURE FILTER
--		The filter removes order procedures that are not in the scope of work. This is defined in the Scope of Work.
--		Excluded Order Procedures: (Adjust this list as needed)
--		'Audiology'                 -- Audiology
--		'Card Rehab'                -- Card Rehab
--		'Cardiac Cath'              -- Cardiac Cath
-- @ORDER TYPE FILTER
--		The filter removes records associated with order types that are not in the scope of work.
--		Excluded Order Types: (Adjust this list as needed)
--		'3'      -- Micro Biology
--		'6'      -- Immunizations
--		'7'      -- Lab
--		'10'     -- Nursing
--		'16'     -- OT (Occupational Therapy)
--		'17'     -- PT (Physical Therapy)
--		'18'     -- Respiratory Care
--		'26'     -- Point of Care
--		'35'     -- Pulmonary Function Test (PFT)
--		'43'     -- IV
--		'46'     -- Dialysis
--		'59'     -- Pathology
--		'63'     -- Point of Care
--		'105'    -- Device Check
--		'999'    -- Medications
--		'1001'   -- OB Ultra Sound
--		'1002'   -- Surgery Request
--		'1007'   -- Lab External
-- @PROCEDURE CATEGORY FILTER
--		The filter removes procedure categories that are not in the scope of work.
--		Excluded Procedure Categories: (Adjust this list as needed)
--		'93'     -- Nursing Assessment Orderables - Once or at Intervals
--		'88'     -- Nursing Informational/Communication Orderables
--		'34'     -- ADT Orderables
--		'94'     -- Nursing Treatment Orderables - Until Discontinued
--		'95'     -- Nursing Treatment Orderables - Once or at Intervals
--		'27'     -- Diet Orderables
--		'84'     -- General Supply & Equipment Orderables
--		'97'     -- Nursing Education Orderables
--		'41'     -- Precaution Orderables
--		'58'     -- Nursing Treatment Orderables - Blood Admin
--		'78'     -- Pharmacy Consult Orderables
--		'83'     -- Nursing Med Communication
--		'150'    -- Neonate Therapies
-- @SCHEDULING STATUS FILTER
--		Scheduling Status listed
--		are to provide a baseline guide to start. Your Epic instance may not contain all listed scheduling statuses
--		or may have a variation of the same. Adjust accordingly.
--	    Examples Include: (Adjust this list as needed)
--		'Appointment Canceled'		--Appointment Canceled
--		,'All Visits Complete'		--All Visits Complete
--		,'All Visits Scheduled'		--All Visits Scheduled
--		,'Called 1x'				--Called 1x
--		,'Called 2x'				--Called 2x
--		,'Called 3x'				--Called 3x
--		,'Clinical Review Pending'	--Clinical Review Pending
--		,'Clinical Review Missing'	--Clinical Review Missing
--		,'Do Not Schedule'			--Do Not Schedule
--		,'External - Info relayed'	--External - Info relayed
--		,'External - Scheduled'		--External - Scheduled
--		,'Information Needed'		--Information Needed
--		,'Not currently credentialed with payor - contracting in process'
--		,'Patient Refusal'			--Patient Refusal
--		,'Pending Final Scheduling'	--Pending Final Scheduling
--		,'1st Txt Msg'				--1st Txt Msg
--		,'2nd Txt Msg'				--2nd Txt Msg
-----------------------------------------------------------------------------------------------------------------
-- Key Highlights:
-- - Temporary Table Creation: Temporary tables like #REFERRAL_INFO are created for intermediate processing.
-- - Data Filtering: Pay special attention to the WHERE clauses used in this query.
--   - Example filters:
--     - Referral status filters to include only specific statuses.
--     - Date range constraints for processing entries within a defined timeframe.
-- - Field Selection: The query retrieves patient demographics, referral statuses, scheduling statuses,
--   and related order information for operational and reporting needs.
-----------------------------------------------------------------------------------------------------------------
*/


IF OBJECT_ID(N'TEMPDB..#REFERRAL_INFO') IS NOT NULL
	DROP TABLE #REFERRAL_INFO;

SELECT 
	REFERRAL.REFERRAL_ID
	,REFERRAL.ENTRY_DATE
	,REFERRAL.CLOSE_DATE
	,REFERRAL_ORDER_ID.ORDER_ID
	,REFERRAL.PAT_ID
	,PATIENT.PAT_MRN_ID
	,CLARITY_EPM.PAYOR_NAME PRIMARY_PAYOR
	,ZC_RFL_STATUS.NAME REFERRAL_STATUS
	,ZC_SCHED_STATUS.NAME SCHED_STATUS
	,PATIENT.PAT_FIRST_NAME
	,PATIENT.PAT_LAST_NAME
	,PATIENT.PAT_MIDDLE_NAME PAT_MIDDLE_NAME
	,ZC_SUFFIX.NAME SUFFIX
	,PATIENT.ADD_LINE_1
	,PATIENT.ADD_LINE_2
	,PATIENT.CITY
	,ZC_STATE.NAME PATIENT_STATE
	,PATIENT.ZIP
	,CAST(PATIENT.BIRTH_DATE AS DATE) PAT_DOB
	,PATIENT.SSN
	,OTHER_COMMUNCTN.OTHER_COMMUNIC_NUM PATIENT_CELL_PHONE
	,PATIENT.HOME_PHONE
	,PATIENT.WORK_PHONE
	,PATIENT.EMAIL_ADDRESS
	,ZC_SEX.NAME PATIENT_SEX
	,ZC_ETHNIC_GROUP.NAME PATIENT_ETNICITY
	,ZC_MARITAL_STATUS.NAME PATIENT_MARITAL_STATUS
	,ZC_PAT_RACE.NAME PATIENT_RACE
	,ZC_LANGUAGE.NAME PATIENT_LANGUAGE
	,CLARITY_SER.PROV_NAME REF_TO_PROV
	,CLARITY_SER_2.NPI REF_TO_PROV_NPI
	,ZC_RSN_FOR_RFL.NAME RSN_FOR_RFL
	,REFERRAL_CVG.CVG_ID
	,PATIENT_4.PAT_LIVING_STAT_C
	,PATIENT_4.SEND_SMS_YN
	,PATIENT_3.NOTIF_PAT_EMAIL_YN
	,ZC_RFL_CLASS.NAME RFL_CLASS
	,ZC_RFL_TYPE.NAME RFL_TYPE
	,ZC_REF_STATUS.NAME REF_STATUS_DETAIL
	,PAT_DEP.SPECIALTY PAT_SPECIALTY
	,CLARITY_LOC.LOC_NAME REF_TO_LOC_NAME
	,CLARITY_POS.ADDRESS_LINE_1 REF_TO_ADDR_1
	,CLARITY_POS.CITY REF_TO_LOC_CITY
	,CLARITY_DEP.DEPARTMENT_NAME REF_TO_DEPT
	,POS_STATE.NAME REF_TO_LOC_STATE
	,CLARITY_POS.ZIP REF_TO_LOC_ZIP
	,CLARITY_POS.AREA_CODE REF_TO_LOC_AREA_CODE
	,CLARITY_POS.PHONE REF_TO_LOC_PHONE

INTO #REFERRAL_INFO

FROM
	REFERRAL
	INNER JOIN REFERRAL_ORDER_ID ON REFERRAL.REFERRAL_ID = REFERRAL_ORDER_ID.REFERRAL_ID
	LEFT JOIN PATIENT ON REFERRAL.PAT_ID = PATIENT.PAT_ID
	LEFT JOIN ZC_SEX ON PATIENT.SEX_C = ZC_SEX.RCPT_MEM_SEX_C
	LEFT JOIN REFERRAL_CVG ON REFERRAL.REFERRAL_ID = REFERRAL_CVG.REFERRAL_ID AND REFERRAL_CVG.LINE = 1
	LEFT JOIN COVERAGE ON REFERRAL_CVG.CVG_ID = COVERAGE.COVERAGE_ID
	LEFT JOIN CLARITY_EPM ON COVERAGE.PAYOR_ID = CLARITY_EPM.PAYOR_ID
	LEFT JOIN ZC_RFL_STATUS ON REFERRAL.RFL_STATUS_C = ZC_RFL_STATUS.RFL_STATUS_C
	LEFT JOIN ZC_SCHED_STATUS ON REFERRAL.SCHED_STATUS_C = ZC_SCHED_STATUS.SCHED_STATUS_C
	LEFT JOIN ZC_SUFFIX ON PATIENT.PAT_NAME_SUFFIX_C = ZC_SUFFIX.SUFFIX_C
	LEFT JOIN ZC_STATE ON PATIENT.STATE_C = ZC_STATE.STATE_C
	LEFT JOIN OTHER_COMMUNCTN ON PATIENT.PAT_ID = OTHER_COMMUNCTN.PAT_ID AND OTHER_COMMUNCTN.OTHER_COMMUNIC_C = 1 AND OTHER_COMMUNCTN.CONTACT_PRIORITY = 1
	LEFT JOIN ZC_ETHNIC_GROUP ON PATIENT.ETHNIC_GROUP_C = ZC_ETHNIC_GROUP.ETHNIC_GROUP_C
	LEFT JOIN ZC_MARITAL_STATUS ON PATIENT.MARITAL_STATUS_C = ZC_MARITAL_STATUS.MARITAL_STATUS_C
	LEFT JOIN PATIENT_RACE ON PATIENT.PAT_ID = PATIENT_RACE.PAT_ID AND PATIENT_RACE.LINE = 1
	LEFT JOIN ZC_PAT_RACE ON PATIENT_RACE.PATIENT_RACE_C = ZC_PAT_RACE.PAT_RACE_C
	LEFT JOIN ZC_LANGUAGE ON PATIENT.LANGUAGE_C = ZC_LANGUAGE.LANGUAGE_C
	LEFT JOIN CLARITY_SER ON REFERRAL.REFERRAL_PROV_ID = CLARITY_SER.PROV_ID
	LEFT JOIN CLARITY_SER_2 ON CLARITY_SER.PROV_ID = CLARITY_SER_2.PROV_ID
	LEFT JOIN ZC_RSN_FOR_RFL ON REFERRAL.RSN_FOR_RFL_C = ZC_RSN_FOR_RFL.RSN_FOR_RFL_C
	LEFT JOIN PATIENT_3 ON PATIENT.PAT_ID = PATIENT_3.PAT_ID
	LEFT JOIN PATIENT_4 ON PATIENT.PAT_ID = PATIENT_4.PAT_ID
	LEFT JOIN ZC_RFL_CLASS ON REFERRAL.RFL_CLASS_C = ZC_RFL_CLASS.RFL_CLASS_C
	LEFT JOIN ZC_RFL_TYPE ON REFERRAL.RFL_TYPE_C = ZC_RFL_TYPE.RFL_TYPE_C
	LEFT JOIN ZC_REF_STATUS ON REFERRAL.REF_STATUS_C = ZC_REF_STATUS.REF_STATUS_C
	LEFT JOIN CLARITY_DEP ON REFERRAL.REFD_TO_DEPT_ID = CLARITY_DEP.DEPARTMENT_ID
	LEFT JOIN CLARITY_DEP PAT_DEP ON PATIENT.CUR_PRIM_LOC_ID = PAT_DEP.DEPARTMENT_ID
	LEFT JOIN CLARITY_LOC ON REFERRAL.REFD_TO_LOC_POS_ID = CLARITY_LOC.LOC_ID
	LEFT JOIN CLARITY_POS ON CLARITY_LOC.LOC_ID = CLARITY_POS.POS_ID
	LEFT JOIN ZC_STATE POS_STATE ON CLARITY_POS.STATE_C = POS_STATE.STATE_C
WHERE
	1=1
	--@DATE FILTER
	AND CAST(REFERRAL.ENTRY_DATE AS DATE) >= DATEADD(DAY,-365,CAST(GETDATE() AS DATE))
	------------------------------------
	--@DECEASED FILTER
	AND PATIENT_4.PAT_LIVING_STAT_C <> 2 --2 Indicates Deceased
	------------------------------------
	--@SCHEDULING STATUS FILTER
	AND ZC_SCHED_STATUS.NAME NOT IN	
	(
		'All Visits Complete'		--All Visits Complete
		,'All Visits Scheduled'		--All Visits Scheduled
		,'Called 1x'				--Called 1x
		,'Called 2x'				--Called 2x
		,'Called 3x'				--Called 3x
		,'Clinical Review Pending'	--Clinical Review Pending
		,'Clinical Review Missing'	--Clinical Review Missing
		,'Do Not Schedule'			--Do Not Schedule
		,'External - Info relayed'	--External - Info relayed
		,'External - Scheduled'		--External - Scheduled
		,'Information Needed'		--Information Needed
		,'Not currently credentialed with payor - contracting in process'
		,'Patient Refusal'			--Patient Refusal
		,'Pending Final Scheduling'	--Pending Final Scheduling
		,'1st Txt Msg'				--1st Txt Msg
		,'2nd Txt Msg'				--2nd Txt Msg
	)
	------------------------------------
----------------------------------------------------------------------------------------------------------

IF OBJECT_ID(N'TEMPDB..#ORDER_INFO') IS NOT NULL
	DROP TABLE #ORDER_INFO;

SELECT
	ORDER_PROC.ORDER_PROC_ID
	,CAST(ORDER_PROC.ORDERING_DATE AS DATE) ORDERING_DATE
	,ORDER_PROC.ORDER_INST
	,ORDER_PROC.DESCRIPTION
	,ORDER_PROC.PAT_ENC_CSN_ID ORDER_CSN
	,ORDER_PROC.PAT_ID
	,ZC_ORDER_STATUS.NAME ORDER_STATUS
	,ZC_ORDER_RESULT_STATUS.NAME ORDER_RESULT_STATUS
	,CLARITY_EAP.PROC_CODE
	,CLARITY_EAP.PROC_NAME
	,CLARITY_EAP.ORDER_DISPLAY_NAME
	,CLARITY_SER.PROV_NAME AUTH_PROVIDER
	,CLARITY_SER_2.NPI ORDER_PROV_NPI
	,CLARITY_SER_SPEC.SPECIALTY ORDER_PROV_SPECIALTY
	,ZC_ORDER_TYPE.NAME ORDER_TYPE
	,CLARITY_DEP.DEPARTMENT_NAME
	,AUTH_DEP.DEPARTMENT_NAME AUTH_DEPT_NAME
	,CLARITY_LOC.LOC_NAME
	,ZC_LOC_RPT_GRP_6.NAME REGION
	-- Adjust RPT_GRP_SEVEN if your Epic instance maps campus to a different reporting group
	,ZC_LOC_RPT_GRP_7.NAME CAMPUS
	,ORDER_PROC.PROC_ID
	,ORDER_PROC.STANDING_EXP_DATE
	,PAT_ENC.HSP_ACCOUNT_ID
	,PAT_ENC.CONTACT_DATE ENCOUNTER_CONTACT_DATE
	,ZC_PAT_CLASS.NAME PAT_CLASS
	,DEP.DEPARTMENT_NAME PAT_ENC_DEP
	,DEP_LOC.LOC_NAME POS_LOC_NAME
	,CLARITY_POS.ADDRESS_LINE_1 LOC_ADD
	,CLARITY_POS.ADDRESS_LINE_2 LOC_ADD_2
	,CLARITY_POS.CITY LOC_CITY
	,POS_STATE.NAME LOC_STATE
	,CLARITY_POS.ZIP LOC_ZIP
	,DEP_LOC.EMERG_PHONE LOC_EMERG_PHONE
	,DEP_LOC.FAX_NUM LOC_FAX_NUM
	,CLARITY_FC.FINANCIAL_CLASS_NAME
	,ZC_APPT_REQ_STATUS.NAME APPT_REQ_STATUS

INTO #ORDER_INFO

FROM
	ORDER_PROC
	LEFT JOIN ZC_ORDER_STATUS ON ORDER_PROC.ORDER_STATUS_C = ZC_ORDER_STATUS.ORDER_STATUS_C
	LEFT JOIN ZC_ORDER_RESULT_STATUS ON ORDER_PROC.ORDER_RESULT_STATUS_C = ZC_ORDER_RESULT_STATUS.ORDER_RESULT_STATUS_C
	LEFT JOIN CLARITY_EAP ON ORDER_PROC.PROC_ID = CLARITY_EAP.PROC_ID
	LEFT JOIN CLARITY_SER ON ORDER_PROC.AUTHRZING_PROV_ID = CLARITY_SER.PROV_ID
	LEFT JOIN CLARITY_SER_2 ON CLARITY_SER.PROV_ID = CLARITY_SER_2.PROV_ID
	LEFT JOIN CLARITY_SER_SPEC ON CLARITY_SER.PROV_ID = CLARITY_SER_SPEC.PROV_ID AND CLARITY_SER_SPEC.LINE = 1
	LEFT JOIN ZC_ORDER_TYPE ON ORDER_PROC.ORDER_TYPE_C = ZC_ORDER_TYPE.ORDER_TYPE_C
	LEFT JOIN ORDER_PROC_2 ON ORDER_PROC.ORDER_PROC_ID = ORDER_PROC_2.ORDER_PROC_ID
	LEFT JOIN CLARITY_DEP ON ORDER_PROC_2.PAT_LOC_ID = CLARITY_DEP.DEPARTMENT_ID
	LEFT JOIN CLARITY_DEP AUTH_DEP ON ORDER_PROC.AUTHRZING_DEPT_ID = AUTH_DEP.DEPARTMENT_ID
	LEFT JOIN CLARITY_LOC ON CLARITY_DEP.REV_LOC_ID = CLARITY_LOC.LOC_ID
	LEFT JOIN ZC_LOC_RPT_GRP_6 ON CLARITY_LOC.RPT_GRP_SIX = ZC_LOC_RPT_GRP_6.RPT_GRP_SIX
	LEFT JOIN ZC_LOC_RPT_GRP_7 ON CLARITY_LOC.RPT_GRP_SEVEN = ZC_LOC_RPT_GRP_7.RPT_GRP_SEVEN
	LEFT JOIN PAT_ENC ON ORDER_PROC.PAT_ENC_CSN_ID = PAT_ENC.PAT_ENC_CSN_ID
	LEFT JOIN ZC_PAT_CLASS ON PAT_ENC.ADT_PAT_CLASS_C = ZC_PAT_CLASS.ADT_PAT_CLASS_C
	LEFT JOIN CLARITY_DEP DEP ON PAT_ENC.DEPARTMENT_ID = DEP.DEPARTMENT_ID
	LEFT JOIN CLARITY_LOC DEP_LOC ON DEP.REV_LOC_ID = DEP_LOC.LOC_ID
	LEFT JOIN CLARITY_POS ON CLARITY_LOC.LOC_ID = CLARITY_POS.POS_ID
	LEFT JOIN ZC_STATE POS_STATE ON CLARITY_POS.STATE_C = POS_STATE.STATE_C
	LEFT JOIN CLARITY_FC ON ORDER_PROC.FIN_CLASS_C = CLARITY_FC.FINANCIAL_CLASS
	LEFT JOIN APPT_REQUEST ON APPT_REQUEST.REQUEST_ID = ORDER_PROC.ORDER_PROC_ID
	LEFT JOIN ZC_APPT_REQ_STATUS ON ZC_APPT_REQ_STATUS.APPT_REQ_STATUS_C = APPT_REQUEST.APPT_REQ_STATUS_C
WHERE
	1=1
	--@ORDER TYPE FILTER-------------------------------------------------------------------------------------------
	AND ORDER_PROC.ORDER_TYPE_C NOT IN (3,6,7,10,16,17,18,26,35,43,46,59,63,105,999,1001,1002,1007)
	---------------------------------------------------------------------------------------------------------------
	--@ORDER CLASS FILTER------------------------------------------------------------------------------------------
	AND ORDER_PROC.ORDER_CLASS_C NOT IN ('1','2','3','4','6','9','10','11','12','13','23','43')
	---------------------------------------------------------------------------------------------------------------
	--@PROCEDURE CATEGORY FILTER-----------------------------------------------------------------------------------
	AND CLARITY_EAP.PROC_CAT_ID NOT IN ('93','88','34','94','95','27','84','97','41','58','78','83','150')
	---------------------------------------------------------------------------------------------------------------
	--@ORDER PROCEDURE FILTER--------------------------------------------------------------------------------------
	--AND ORDER_PROC.ORDER_PROC_ID IN 
	--(
	--SELECT DISTINCT
	--	ORDER_ID
	--FROM
	--	#REFERRAL_INFO
	--)
	--AND ZC_ORDER_TYPE.NAME NOT IN 
	--(
	--'Audiology'					--Audiology
	--,'Card Rehab'					--Card Rehab
	--,'Cardiac Cath'				--Cardiac Cath
	--,'Cardiac Services'			--Cardiac Services
	--)
	---------------------------------------------------------------------------------------------------------------
	--@ORDER EXPIRED FILTER----------------------------------------------------------------------------------------
	AND (ORDER_PROC.STANDING_EXP_DATE IS NULL OR ORDER_PROC.STANDING_EXP_DATE >= CAST(GETDATE() AS DATE))
----------------------------------------------------------------------------------------------------------

IF OBJECT_ID(N'TEMPDB..#SCHEDULE_INFO') IS NOT NULL
	DROP TABLE #SCHEDULE_INFO;

SELECT 
	F_SCHED_APPT.*
	,ZC_APPT_STATUS.NAME APPT_STATUS

INTO #SCHEDULE_INFO

FROM
	F_SCHED_APPT
	LEFT JOIN ZC_APPT_STATUS ON F_SCHED_APPT.APPT_STATUS_C = ZC_APPT_STATUS.APPT_STATUS_C
WHERE
	F_SCHED_APPT.REFERRAL_ID IN
	(
	SELECT DISTINCT
		REFERRAL_ID
	FROM
		#REFERRAL_INFO
	)

----------------------------------------------------------------------------------------------------------

IF OBJECT_ID(N'TEMPDB..#PAT_ENC_DX') IS NOT NULL
	DROP TABLE #PAT_ENC_DX;

SELECT 
	PAT_ENC_DX.PAT_ENC_CSN_ID
	,PAT_ENC.REFERRAL_ID
	,ISNULL(CLARITY_EDG.CURRENT_ICD10_LIST,CLARITY_EDG.CURRENT_ICD9_LIST) DX_CODE
	,CLARITY_EDG.DX_NAME

INTO #PAT_ENC_DX

FROM
	PAT_ENC_DX
	INNER JOIN PAT_ENC ON PAT_ENC_DX.PAT_ENC_CSN_ID = PAT_ENC.PAT_ENC_CSN_ID
	LEFT JOIN CLARITY_EDG ON PAT_ENC_DX.DX_ID = CLARITY_EDG.DX_ID
WHERE
	PAT_ENC.PAT_ENC_CSN_ID IN
	(
	SELECT DISTINCT
		ORDER_CSN
	FROM
		#ORDER_INFO
	)
	AND PAT_ENC_DX.PRIMARY_DX_YN = 'Y'

----------------------------------------------------------------------------------------------------------

IF OBJECT_ID(N'TEMPDB..#ORDER_PROC_CPT') IS NOT NULL
	DROP TABLE #ORDER_PROC_CPT;

SELECT *

INTO #ORDER_PROC_CPT

FROM
	(
	SELECT
		PROC_ID
		,CONTACT_DATE
		,CPT_CODE
		,DESCRIPTION
		,NAME_HISTORY
		,ROW_NUMBER() OVER (PARTITION BY PROC_ID ORDER BY CONTACT_DATE DESC) RN
	FROM
		CLARITY_EAP_OT
	WHERE
		PROC_ID IN
		(
		SELECT DISTINCT
			PROC_ID
		FROM
			#ORDER_INFO
		)
	) A
WHERE
	A.RN = 1

----------------------------------------------------------------------------------------------------------

IF OBJECT_ID(N'TEMPDB..#CVG_INFO') IS NOT NULL
	DROP TABLE #CVG_INFO;

SELECT DISTINCT
	V_COVERAGE_PAYOR_PLAN.BENEFIT_PLAN_NAME PAT_BENEFIT_PLAN_NAME
	,V_COVERAGE_PAYOR_PLAN.PAYOR_NAME PAT_PAYOR_NAME
	,COVERAGE.SUBSCR_NAME
	,COVERAGE.SUBSCR_NUM
	,COVERAGE.GROUP_NUM
	,COVERAGE_MEM_LIST.MEM_EFF_FROM_DATE PAT_MEM_EFF_FROM_DATE
	,COVERAGE_MEM_LIST.MEM_EFF_TO_DATE PAT_MEM_EFF_TO_DATE
	,CLARITY_EPP.BP_ADDR_LINE1
	,CLARITY_EPP.BP_ADDR_LINE2
	,CLARITY_EPP.BP_CITY
	,ZC_STATE.NAME BP_STATE
	,CLARITY_EPP.BP_ZIP
	,CLARITY_EPP.BP_PHONE
	,CLARITY_EPP.BP_FAX
	,COVERAGE_MEM_LIST.COVERAGE_ID PAT_COVERAGE_ID
	,COVERAGE_MEM_LIST.PAT_ID
	,COVERAGE_MEM_LIST.COVERAGE_ID
	,V_COVERAGE_PAYOR_PLAN.PAYOR_ID
	,ROW_NUMBER() OVER (PARTITION BY COVERAGE_MEM_LIST.PAT_ID,COVERAGE_MEM_LIST.COVERAGE_ID ORDER BY COVERAGE_MEM_LIST.MEM_EFF_TO_DATE,PAT_CVG_FILE_ORDER.FILING_ORDER DESC) RN

INTO #CVG_INFO

FROM
	COVERAGE_MEM_LIST
	LEFT JOIN PAT_CVG_FILE_ORDER ON PAT_CVG_FILE_ORDER.PAT_ID = COVERAGE_MEM_LIST.PAT_ID AND PAT_CVG_FILE_ORDER.COVERAGE_ID = COVERAGE_MEM_LIST.COVERAGE_ID
	LEFT JOIN V_COVERAGE_PAYOR_PLAN ON COVERAGE_MEM_LIST.COVERAGE_ID = V_COVERAGE_PAYOR_PLAN.COVERAGE_ID
	LEFT JOIN COVERAGE ON COVERAGE_MEM_LIST.COVERAGE_ID = COVERAGE.COVERAGE_ID
	LEFT JOIN CLARITY_EPP ON V_COVERAGE_PAYOR_PLAN.PAYOR_ID = CLARITY_EPP.PAYOR_ID AND V_COVERAGE_PAYOR_PLAN.BENEFIT_PLAN_ID = CLARITY_EPP.BENEFIT_PLAN_ID
	LEFT JOIN ZC_STATE ON CLARITY_EPP.BP_STATE_C = ZC_STATE.STATE_C
WHERE
	1=1
	AND 
	(
	COVERAGE_MEM_LIST.MEM_EFF_TO_DATE IS NULL
	OR  
	CAST(COVERAGE_MEM_LIST.MEM_EFF_TO_DATE AS DATE) >= DATEADD(DAY,-365,CAST(GETDATE() AS DATE))
	)
	AND COVERAGE_MEM_LIST.COVERAGE_ID IN
	(
	SELECT
		CVG_ID
	FROM
		#REFERRAL_INFO
	)

----------------------------------------------------------------------------------------------------------

SELECT DISTINCT
	-- Patient
	#REFERRAL_INFO.PAT_ID AS								'patient_id'
	,#ORDER_INFO.ORDER_CSN AS								'encounter_csn_id'
	,#ORDER_INFO.HSP_ACCOUNT_ID AS							'account_number'
	,#REFERRAL_INFO.PAT_MRN_ID AS							'patient_mrn'
	,#REFERRAL_INFO.PAT_FIRST_NAME AS						'patient_first_name'
	,#REFERRAL_INFO.PAT_LAST_NAME AS						'patient_last_name'
	,#REFERRAL_INFO.PAT_MIDDLE_NAME AS						'patient_middle_name'
	,#REFERRAL_INFO.SUFFIX AS								'patient_suffix'
	,#REFERRAL_INFO.ADD_LINE_1 AS							'patient_address1'
	,#REFERRAL_INFO.ADD_LINE_2 AS							'patient_address2'
	,#REFERRAL_INFO.CITY AS									'patient_city'
	,#REFERRAL_INFO.PATIENT_STATE AS						'patient_state'
	,#REFERRAL_INFO.ZIP AS									'patient_zip'
	,#REFERRAL_INFO.PAT_DOB AS								'patient_birth_date'
	,#REFERRAL_INFO.SSN AS									'patient_ssn'
	,#REFERRAL_INFO.PATIENT_CELL_PHONE AS					'patient_mobile_phone'
	,#REFERRAL_INFO.HOME_PHONE AS							'patient_home_phone'
	,#REFERRAL_INFO.WORK_PHONE AS							'patient_work_phone'
	,#REFERRAL_INFO.EMAIL_ADDRESS AS						'patient_email'
	,#REFERRAL_INFO.PATIENT_SEX AS							'patient_gender'
	,#REFERRAL_INFO.PATIENT_ETNICITY AS						'patient_ethnicity'
	,#REFERRAL_INFO.PATIENT_MARITAL_STATUS AS				'patient_marital_status'
	,#REFERRAL_INFO.PATIENT_RACE AS							'patient_primary_race'
	,#ORDER_INFO.PAT_CLASS AS								'patient_class'
	,#REFERRAL_INFO.PATIENT_LANGUAGE AS						'patient_language'
	,#REFERRAL_INFO.PAT_LIVING_STAT_C AS					'patient_deceased_flag'
	,#REFERRAL_INFO.SEND_SMS_YN AS							'patient_opt_out_sms'
	,#REFERRAL_INFO.NOTIF_PAT_EMAIL_YN AS					'patient_opt_out_email'
	,#REFERRAL_INFO.PAT_SPECIALTY AS						'specialty'
	-- Encounter
	,#ORDER_INFO.PAT_ENC_DEP AS								'encounter_department'
	,CAST(#ORDER_INFO.ENCOUNTER_CONTACT_DATE AS DATE) AS	'encounter_contact_date'
	-- Referral
	,#REFERRAL_INFO.REFERRAL_ID AS							'referral_id'
	,#REFERRAL_INFO.RFL_CLASS AS							'referral_class'
	,#REFERRAL_INFO.RFL_TYPE AS								'clinical_category'
	,#REFERRAL_INFO.REFERRAL_STATUS AS						'referral_status'
	,#REFERRAL_INFO.REF_STATUS_DETAIL AS					'referral_status_detail'
	,CAST(#REFERRAL_INFO.ENTRY_DATE AS DATE) AS				'referral_entry_date'
	,CAST(#REFERRAL_INFO.CLOSE_DATE AS DATE) AS				'referral_close_date'
	-- Order
	,#ORDER_INFO.ORDER_PROC_ID AS							'order_id'
	,#ORDER_INFO.ORDER_TYPE AS								'order_type'
	,CAST(#ORDER_INFO.ORDERING_DATE AS DATE) AS				'order_date'
	,CAST(#ORDER_INFO.ORDER_INST AS DATE) AS				'order_entry_date'
	,#ORDER_INFO.STANDING_EXP_DATE AS						'order_expiration_date'
	,#ORDER_INFO.PROC_NAME AS								'order_name'
	,#ORDER_INFO.AUTH_PROVIDER AS							'order_provider_name'
	,#ORDER_INFO.ORDER_PROV_NPI AS							'order_provider_npi'
	,#ORDER_INFO.ORDER_DISPLAY_NAME AS						'order_mnemonic'
	,#ORDER_INFO.PROC_CODE AS								'order_procedure'
	,#ORDER_INFO.DESCRIPTION AS								'order_description'
	,#ORDER_INFO.ORDER_STATUS AS							'order_status'
	,#ORDER_INFO.ORDER_RESULT_STATUS AS						'order_result_status'
	,DATEDIFF(DAY,#ORDER_INFO.ORDERING_DATE,GETDATE()) AS	'order_age'
	,#ORDER_INFO.AUTH_DEPT_NAME AS							'ordering_department_name'
	,#ORDER_INFO.ORDER_PROV_SPECIALTY AS					'ordering_provider_specialty'
	-- Procedure / Diagnosis
	,#ORDER_INFO.PROC_ID AS								'procedure_id'
	,#ORDER_INFO.PROC_CODE AS								'procedure_code'
	,#PAT_ENC_DX.DX_CODE AS								'dx_code'
	,#PAT_ENC_DX.DX_NAME AS								'dx_description'
	,#ORDER_PROC_CPT.CPT_CODE AS							'cpt_code'
	,#ORDER_PROC_CPT.DESCRIPTION AS							'cpt_procedure_description'
	,#ORDER_PROC_CPT.NAME_HISTORY AS						'cpt_description'
	,#ORDER_PROC_CPT.CONTACT_DATE AS						'max_procedure_contact_date'
	-- Appointment / Scheduling
	,#SCHEDULE_INFO.PAT_ENC_CSN_ID AS						'appointment_id'
	,#SCHEDULE_INFO.CONTACT_DATE AS							'appointment_date'
	,#SCHEDULE_INFO.APPT_STATUS AS							'appointment_status'
	,#ORDER_INFO.APPT_REQ_STATUS AS							'appointment_request_status'
	,#REFERRAL_INFO.SCHED_STATUS AS							'scheduling_status'
	-- Insurance
	,#ORDER_INFO.FINANCIAL_CLASS_NAME AS						'financial_class'
	,#CVG_INFO.PAT_BENEFIT_PLAN_NAME AS						'insurance_1_code'
	,#CVG_INFO.PAT_PAYOR_NAME AS							'insurance_1_payor_name'
	,#CVG_INFO.SUBSCR_NAME AS								'insurance_1_policy_holder'
	,#CVG_INFO.SUBSCR_NAME AS								'insurance_1_member_name'
	,#CVG_INFO.SUBSCR_NUM AS								'insurance_1_member_id'
	,#CVG_INFO.GROUP_NUM AS									'insurance_1_group_number'
	,#CVG_INFO.PAT_MEM_EFF_FROM_DATE AS						'insurance_1_effective_date'
	,#CVG_INFO.PAT_MEM_EFF_TO_DATE AS						'insurance_1_expiration_date'
	,#CVG_INFO.BP_ADDR_LINE1 AS								'insurance_1_address1'
	,#CVG_INFO.BP_ADDR_LINE2 AS								'insurance_1_address2'
	,#CVG_INFO.BP_CITY AS									'insurance_1_city'
	,#CVG_INFO.BP_STATE AS									'insurance_1_state'
	,#CVG_INFO.BP_ZIP AS									'insurance_1_zip'
	,#CVG_INFO.BP_PHONE AS									'insurance_1_phone'
	,#CVG_INFO.BP_FAX AS									'insurance_1_fax'
	-- Facilities
	,#ORDER_INFO.REGION AS									'region'
	,#ORDER_INFO.CAMPUS AS									'campus'
	,#ORDER_INFO.LOC_NAME AS								'location'
	,#ORDER_INFO.POS_LOC_NAME AS							'referred_from_location'
	,#ORDER_INFO.LOC_ADD AS									'referred_from_location_address1'
	,#ORDER_INFO.LOC_ADD_2 AS								'referred_from_location_address2'
	,#ORDER_INFO.LOC_CITY AS								'referred_from_location_city'
	,#ORDER_INFO.LOC_STATE AS								'referred_from_location_state'
	,#ORDER_INFO.LOC_ZIP AS									'referred_from_location_zip'
	,#ORDER_INFO.LOC_EMERG_PHONE AS							'referred_from_location_phone_number'
	,#ORDER_INFO.LOC_FAX_NUM AS								'referred_from_location_fax_number'
	-- Referred To
	,#REFERRAL_INFO.REF_TO_PROV_NPI AS						'referred_to_provider_npi'
	,#REFERRAL_INFO.REF_TO_PROV AS							'referred_to_provider_name'
	,#REFERRAL_INFO.RSN_FOR_RFL AS							'referred_to_reason'
	,#REFERRAL_INFO.REF_TO_LOC_NAME AS						'referred_to_location'
	,#REFERRAL_INFO.REF_TO_ADDR_1 AS						'referred_to_location_address'
	,#REFERRAL_INFO.REF_TO_LOC_CITY AS						'referred_to_location_city'
	,#REFERRAL_INFO.REF_TO_DEPT AS							'referred_to_department'
	,#REFERRAL_INFO.REF_TO_LOC_STATE AS						'referred_to_location_state'
	,#REFERRAL_INFO.REF_TO_LOC_ZIP AS						'referred_to_location_zip'
	,#REFERRAL_INFO.REF_TO_LOC_AREA_CODE AS					'referred_to_location_area_code'
	,#REFERRAL_INFO.REF_TO_LOC_PHONE AS						'referred_to_location_phone_number'
FROM
	#REFERRAL_INFO
	INNER JOIN #ORDER_INFO ON #REFERRAL_INFO.ORDER_ID = #ORDER_INFO.ORDER_PROC_ID
	LEFT JOIN #SCHEDULE_INFO ON #REFERRAL_INFO.REFERRAL_ID = #SCHEDULE_INFO.REFERRAL_ID
	LEFT JOIN #PAT_ENC_DX ON #ORDER_INFO.ORDER_CSN = #PAT_ENC_DX.PAT_ENC_CSN_ID
	LEFT JOIN #ORDER_PROC_CPT ON #ORDER_INFO.PROC_ID = #ORDER_PROC_CPT.PROC_ID
	LEFT JOIN #CVG_INFO ON #REFERRAL_INFO.CVG_ID = #CVG_INFO.COVERAGE_ID AND #CVG_INFO.PAT_ID = #REFERRAL_INFO.PAT_ID AND #CVG_INFO.RN = 1
		AND 
		(
			(#CVG_INFO.PAT_MEM_EFF_FROM_DATE <= #ORDER_INFO.ORDERING_DATE AND #CVG_INFO.PAT_MEM_EFF_TO_DATE >= #ORDER_INFO.ORDERING_DATE)
			OR(#CVG_INFO.PAT_MEM_EFF_FROM_DATE <= #ORDER_INFO.ORDERING_DATE AND #CVG_INFO.PAT_MEM_EFF_TO_DATE IS NULL)
		)
WHERE
	1=1
	AND #SCHEDULE_INFO.CONTACT_DATE IS NULL
ORDER BY 3,1

----------------------------------------------------------------------------------------------------------

DROP TABLE #REFERRAL_INFO;
DROP TABLE #ORDER_INFO;
DROP TABLE #SCHEDULE_INFO;
DROP TABLE #PAT_ENC_DX;
DROP TABLE #ORDER_PROC_CPT;
DROP TABLE #CVG_INFO;
