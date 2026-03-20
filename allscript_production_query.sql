/*
-----------------------------------------------------------------------------------------------------------------
-- Abax Healthcare, Inc
-- Contact: Jay Dalke
-- Modified: 03/20/2026
-- Email: support@abaxhealth.com
-- Please reach out to Jay Dalke for any assistance with this query.
-- Query support is part of Abax Health's comprehensive implementation process.
-----------------------------------------------------------------------------------------------------------------
-- Purpose:
-- This query extracts detailed patient, order, and referral information for Abax Health
-- from an Allscripts/Altera Sunrise Clinical Manager (SCM) hosted environment.
-- It utilizes temporary tables and joins multiple data sources for comprehensive reporting.
--
-- Platform:  Allscripts / Altera — Sunrise Clinical Manager (hosted)
-- Primary Key: VisitID (HPatientVisit) is the primary unique identifier for this client.
-----------------------------------------------------------------------------------------------------------------
-- Known Gaps:
-- @NO-SHOW FILTER
--      The client's no-show filter is not being used properly. We still capture the
--      NoShowIndicator and appointment status fields so that downstream consumers
--      can apply their own logic. The filter is present but commented out — enable
--      and adjust once the client confirms the correct no-show handling.
-----------------------------------------------------------------------------------------------------------------
-- Filters:
-- @DATE FILTER
--      Captures all referrals from the past 365 days via ReferralEntryDateTime.
-- @DECEASED FILTER
--      Excludes deceased patients (DeceasedIndicator = 'Y' or DeceasedDateTime IS NOT NULL).
-- @ORDER CLASS FILTER
--      Removes order classes outside the scope of work.
--      Excluded OrderClassCode values: (Adjust as needed per Sunrise dictionary)
--      'NORMAL','POC','HISTORICAL','LABCOLLECT','OTC','PHONEIN',
--      'NOPRINT','UNITCOLLECT','PRINT','CLINICPERF','CLINICCOLLECT'
-- @ORDER TYPE FILTER
--      Removes order types outside the scope of work.
--      Excluded OrderTypeCode values: (Adjust as needed per Sunrise dictionary)
--      'MICROBIOLOGY','IMMUNIZATION','LAB','NURSING','OT','PT',
--      'RESPCARE','POC','PFT','IV','DIALYSIS','PATHOLOGY',
--      'DEVICECHECK','MEDICATION','OBULTRASOUND','SURGERYREQ','LABEXTERNAL'
-- @PROCEDURE CATEGORY FILTER
--      Removes procedure categories outside the scope of work.
--      Excluded ProcedureCategoryCode values: (Adjust as needed)
-- @ORDER EXPIRED FILTER
--      Excludes orders whose ExpirationDateTime has passed.
-- @SCHEDULING STATUS FILTER
--      Scheduling statuses listed below are a baseline guide. Your Sunrise instance may
--      not contain all listed statuses or may use variations. Adjust accordingly.
--      Examples: (Adjust this list as needed)
--      'All Visits Complete'
--      ,'All Visits Scheduled'
--      ,'Called 1x'
--      ,'Called 2x'
--      ,'Called 3x'
--      ,'Clinical Review Pending'
--      ,'Clinical Review Missing'
--      ,'Do Not Schedule'
--      ,'External - Info relayed'
--      ,'External - Scheduled'
--      ,'Information Needed'
--      ,'Patient Refusal'
--      ,'Pending Final Scheduling'
-- @NO-SHOW FILTER (known gap — see above)
--      HAppointment.NoShowIndicator or AppointmentStatusCode = 'NO SHOW'
--      Currently captured but NOT filtered. Enable once the client confirms proper usage.
-----------------------------------------------------------------------------------------------------------------
-- Key Highlights:
-- - Temporary tables are created for intermediate processing (#REFERRAL_INFO, #ORDER_INFO, etc.).
-- - VisitID (HPatientVisit) is the primary unique identifier joining orders and referrals.
-- - Appointment status AND scheduling status are both returned in the final output.
-- - NoShowIndicator is captured for downstream no-show analysis despite the known filter gap.
-- - Field selection mirrors the Epic Clarity production query for consistency across EHR platforms.
-----------------------------------------------------------------------------------------------------------------
-- Allscripts/Altera Sunrise Table Reference:
--   HPerson              — Person demographics
--   HPatient             — Patient-specific data (MRN)
--   HPersonAddress       — Patient address
--   HPatientVisit        — Visit / Encounter (VisitID = primary key)
--   HOrder               — Orders
--   HReferral            — Referrals
--   HStaff               — Providers / Staff
--   HStaffSpecialty      — Provider specialties
--   HAppointment         — Appointments / Scheduling
--   HPatientVisitDiagnosis — Visit diagnoses
--   HPatientInsurance    — Patient insurance
--   HInsuranceCompany    — Insurance company master
--   HFacility            — Facility master
--   HDepartment          — Department master
--   DOrderCatalog        — Order catalog / procedure dictionary
--   DMasterItem          — Master dictionary (status codes, coded values)
-----------------------------------------------------------------------------------------------------------------
*/


-- =============================================================================================================
-- #REFERRAL_INFO — Referral and patient demographic data
-- =============================================================================================================

IF OBJECT_ID(N'TEMPDB..#REFERRAL_INFO') IS NOT NULL
	DROP TABLE #REFERRAL_INFO;

SELECT
	ref.ReferralID
	,ref.ReferralEntryDateTime
	,ref.ReferralCloseDateTime
	,ref.OrderID
	,ref.PatientID
	,ref.VisitID
	,pat.MedicalRecordNumber                            AS PAT_MRN
	,ins_co.CompanyName                                 AS PRIMARY_PAYOR
	,ref_status.Description                             AS REFERRAL_STATUS
	,sched_status.Description                           AS SCHED_STATUS
	,per.FirstName                                      AS PAT_FIRST_NAME
	,per.LastName                                       AS PAT_LAST_NAME
	,per.MiddleName                                     AS PAT_MIDDLE_NAME
	,suffix.Description                                 AS SUFFIX
	,addr.AddressLine1                                  AS ADD_LINE_1
	,addr.AddressLine2                                  AS ADD_LINE_2
	,addr.City                                          AS CITY
	,addr.State                                         AS PATIENT_STATE
	,addr.ZipCode                                       AS ZIP
	,CAST(per.DateOfBirth AS DATE)                      AS PAT_DOB
	,per.SSN
	,cell.PhoneNumber                                   AS PATIENT_CELL_PHONE
	,home.PhoneNumber                                   AS HOME_PHONE
	,work.PhoneNumber                                   AS WORK_PHONE
	,per.EmailAddress                                   AS EMAIL_ADDRESS
	,sex.Description                                    AS PATIENT_SEX
	,ethnic.Description                                 AS PATIENT_ETHNICITY
	,marital.Description                                AS PATIENT_MARITAL_STATUS
	,race.Description                                   AS PATIENT_RACE
	,lang.Description                                   AS PATIENT_LANGUAGE
	,ref_to_staff.LastName + ', ' + ref_to_staff.FirstName AS REF_TO_PROV
	,ref_to_staff.NPI                                   AS REF_TO_PROV_NPI
	,ref_reason.Description                             AS RSN_FOR_RFL
	,pri_ins.PatientInsuranceID                         AS CVG_ID
	,per.DeceasedIndicator                              AS PAT_LIVING_STAT
	,per.DeceasedDateTime                               AS PAT_DECEASED_DATETIME
	,per.SMSOptInIndicator                              AS SEND_SMS_YN
	,per.EmailOptInIndicator                            AS NOTIF_PAT_EMAIL_YN
	,rfl_class.Description                              AS RFL_CLASS
	,rfl_type.Description                               AS RFL_TYPE
	,ref_detail_status.Description                      AS REF_STATUS_DETAIL
	,pat_dept.SpecialtyDescription                      AS PAT_SPECIALTY
	,ref_fac.FacilityName                               AS REF_TO_LOC_NAME
	,ref_fac.AddressLine1                               AS REF_TO_ADDR_1
	,ref_fac.City                                       AS REF_TO_LOC_CITY
	,ref_dept.DepartmentName                            AS REF_TO_DEPT
	,ref_fac.State                                      AS REF_TO_LOC_STATE
	,ref_fac.ZipCode                                    AS REF_TO_LOC_ZIP
	,ref_fac.PhoneNumber                                AS REF_TO_LOC_PHONE
	,ref_dept.PhoneNumber                               AS REF_TO_DEPT_PHONE

INTO #REFERRAL_INFO

FROM
	HReferral ref
	INNER JOIN HOrder ord_link ON ref.OrderID = ord_link.OrderID
	LEFT JOIN HPatient pat ON ref.PatientID = pat.PatientID
	LEFT JOIN HPerson per ON pat.PatientID = per.PersonID
	LEFT JOIN HPersonAddress addr
		ON per.PersonID = addr.PersonID
		AND addr.AddressTypeCode = 'HOME'
		AND addr.SequenceNumber = 1
	LEFT JOIN HPersonPhone cell
		ON per.PersonID = cell.PersonID
		AND cell.PhoneTypeCode = 'CELL'
		AND cell.SequenceNumber = 1
	LEFT JOIN HPersonPhone home
		ON per.PersonID = home.PersonID
		AND home.PhoneTypeCode = 'HOME'
		AND home.SequenceNumber = 1
	LEFT JOIN HPersonPhone work
		ON per.PersonID = work.PersonID
		AND work.PhoneTypeCode = 'WORK'
		AND work.SequenceNumber = 1
	LEFT JOIN DMasterItem sex
		ON per.GenderCode = sex.Code
		AND sex.CategoryCode = 'GENDER'
	LEFT JOIN DMasterItem ethnic
		ON per.EthnicityCode = ethnic.Code
		AND ethnic.CategoryCode = 'ETHNICITY'
	LEFT JOIN DMasterItem marital
		ON per.MaritalStatusCode = marital.Code
		AND marital.CategoryCode = 'MARITAL_STATUS'
	LEFT JOIN DMasterItem race
		ON per.RaceCode = race.Code
		AND race.CategoryCode = 'RACE'
	LEFT JOIN DMasterItem lang
		ON per.LanguageCode = lang.Code
		AND lang.CategoryCode = 'LANGUAGE'
	LEFT JOIN DMasterItem suffix
		ON per.SuffixCode = suffix.Code
		AND suffix.CategoryCode = 'SUFFIX'
	LEFT JOIN HPatientInsurance pri_ins
		ON ref.PatientID = pri_ins.PatientID
		AND pri_ins.FilingOrder = 1
	LEFT JOIN HInsuranceCompany ins_co
		ON pri_ins.InsuranceCompanyID = ins_co.InsuranceCompanyID
	LEFT JOIN DMasterItem ref_status
		ON ref.ReferralStatusCode = ref_status.Code
		AND ref_status.CategoryCode = 'REFERRAL_STATUS'
	LEFT JOIN DMasterItem sched_status
		ON ref.SchedulingStatusCode = sched_status.Code
		AND sched_status.CategoryCode = 'SCHEDULING_STATUS'
	LEFT JOIN HStaff ref_to_staff
		ON ref.ReferredToProviderID = ref_to_staff.StaffID
	LEFT JOIN DMasterItem ref_reason
		ON ref.ReferralReasonCode = ref_reason.Code
		AND ref_reason.CategoryCode = 'REFERRAL_REASON'
	LEFT JOIN DMasterItem rfl_class
		ON ref.ReferralClassCode = rfl_class.Code
		AND rfl_class.CategoryCode = 'REFERRAL_CLASS'
	LEFT JOIN DMasterItem rfl_type
		ON ref.ReferralTypeCode = rfl_type.Code
		AND rfl_type.CategoryCode = 'REFERRAL_TYPE'
	LEFT JOIN DMasterItem ref_detail_status
		ON ref.ReferralDetailStatusCode = ref_detail_status.Code
		AND ref_detail_status.CategoryCode = 'REFERRAL_DETAIL_STATUS'
	LEFT JOIN HDepartment ref_dept
		ON ref.ReferredToDepartmentID = ref_dept.DepartmentID
	LEFT JOIN HFacility ref_fac
		ON ref.ReferredToFacilityID = ref_fac.FacilityID
	LEFT JOIN HDepartment pat_dept
		ON pat.PrimaryDepartmentID = pat_dept.DepartmentID
WHERE
	1=1
	------------------------------------
	--@DATE FILTER
	AND CAST(ref.ReferralEntryDateTime AS DATE) >= DATEADD(DAY, -365, CAST(GETDATE() AS DATE))
	------------------------------------
	--@DECEASED FILTER
	AND (per.DeceasedIndicator IS NULL OR per.DeceasedIndicator <> 'Y')
	AND per.DeceasedDateTime IS NULL
	------------------------------------
	--@SCHEDULING STATUS FILTER
	AND ISNULL(sched_status.Description, '') NOT IN
	(
		'All Visits Complete'
		,'All Visits Scheduled'
		,'Called 1x'
		,'Called 2x'
		,'Called 3x'
		,'Clinical Review Pending'
		,'Clinical Review Missing'
		,'Do Not Schedule'
		,'External - Info relayed'
		,'External - Scheduled'
		,'Information Needed'
		,'Not currently credentialed with payor - contracting in process'
		,'Patient Refusal'
		,'Pending Final Scheduling'
		,'1st Txt Msg'
		,'2nd Txt Msg'
	)
	------------------------------------

-- =============================================================================================================
-- #ORDER_INFO — Order, provider, department, and facility data
-- =============================================================================================================

IF OBJECT_ID(N'TEMPDB..#ORDER_INFO') IS NOT NULL
	DROP TABLE #ORDER_INFO;

SELECT
	ord.OrderID
	,ord.VisitID
	,CAST(ord.OrderDateTime AS DATE)                    AS ORDERING_DATE
	,ord.EnteredDateTime                                AS ORDER_INST
	,ord.OrderDescription                               AS DESCRIPTION
	,ord.VisitID                                        AS ORDER_VISIT_ID
	,ord.PatientID
	,ord_status.Description                             AS ORDER_STATUS
	,ord_result.Description                             AS ORDER_RESULT_STATUS
	,cat.OrderCode                                      AS PROC_CODE
	,cat.OrderName                                      AS PROC_NAME
	,cat.OrderDisplayName                               AS ORDER_DISPLAY_NAME
	,auth_staff.LastName + ', ' + auth_staff.FirstName  AS AUTH_PROVIDER
	,auth_staff.NPI                                     AS ORDER_PROV_NPI
	,auth_spec.SpecialtyDescription                     AS ORDER_PROV_SPECIALTY
	,ord_type.Description                               AS ORDER_TYPE
	,ord_dept.DepartmentName                            AS DEPARTMENT_NAME
	,auth_dept.DepartmentName                           AS AUTH_DEPT_NAME
	,fac.FacilityName                                   AS LOC_NAME
	,fac.RegionCode                                     AS REGION
	,fac.CampusCode                                     AS CAMPUS
	,cat.OrderCatalogID                                 AS PROC_ID
	,ord.ExpirationDateTime                             AS STANDING_EXP_DATE
	,visit.AccountNumber                                AS HSP_ACCOUNT_ID
	,CAST(visit.AdmitDateTime AS DATE)                  AS ENCOUNTER_CONTACT_DATE
	,pat_class.Description                              AS PAT_CLASS
	,enc_dept.DepartmentName                            AS PAT_ENC_DEP
	,enc_fac.FacilityName                               AS POS_LOC_NAME
	,enc_fac.AddressLine1                               AS LOC_ADD
	,enc_fac.City                                       AS LOC_CITY
	,enc_fac.State                                      AS LOC_STATE
	,enc_fac.ZipCode                                    AS LOC_ZIP
	,enc_fac.PhoneNumber                                AS LOC_PHONE
	,enc_fac.FaxNumber                                  AS LOC_FAX_NUM
	,fin_class.Description                              AS FINANCIAL_CLASS_NAME
	,appt_req_status.Description                        AS APPT_REQ_STATUS
	,appt_status.Description                            AS APPT_STATUS
	,appt.SchedulingStatusCode                          AS APPT_SCHED_STATUS_CODE
	,appt_sched.Description                             AS APPT_SCHED_STATUS
	,appt.NoShowIndicator                               AS NO_SHOW_FLAG

INTO #ORDER_INFO

FROM
	HOrder ord
	LEFT JOIN DMasterItem ord_status
		ON ord.OrderStatusCode = ord_status.Code
		AND ord_status.CategoryCode = 'ORDER_STATUS'
	LEFT JOIN DMasterItem ord_result
		ON ord.OrderResultStatusCode = ord_result.Code
		AND ord_result.CategoryCode = 'ORDER_RESULT_STATUS'
	LEFT JOIN DOrderCatalog cat
		ON ord.OrderCatalogMasterItemID = cat.OrderCatalogID
	LEFT JOIN HStaff auth_staff
		ON ord.AuthorizingProviderID = auth_staff.StaffID
	LEFT JOIN HStaffSpecialty auth_spec
		ON auth_staff.StaffID = auth_spec.StaffID
		AND auth_spec.SequenceNumber = 1
	LEFT JOIN DMasterItem ord_type
		ON ord.OrderTypeCode = ord_type.Code
		AND ord_type.CategoryCode = 'ORDER_TYPE'
	LEFT JOIN HDepartment ord_dept
		ON ord.DepartmentID = ord_dept.DepartmentID
	LEFT JOIN HDepartment auth_dept
		ON ord.AuthorizingDepartmentID = auth_dept.DepartmentID
	LEFT JOIN HFacility fac
		ON ord_dept.FacilityID = fac.FacilityID
	LEFT JOIN HPatientVisit visit
		ON ord.VisitID = visit.VisitID
	LEFT JOIN DMasterItem pat_class
		ON visit.PatientClassCode = pat_class.Code
		AND pat_class.CategoryCode = 'PATIENT_CLASS'
	LEFT JOIN HDepartment enc_dept
		ON visit.DepartmentID = enc_dept.DepartmentID
	LEFT JOIN HFacility enc_fac
		ON enc_dept.FacilityID = enc_fac.FacilityID
	LEFT JOIN DMasterItem fin_class
		ON visit.FinancialClassCode = fin_class.Code
		AND fin_class.CategoryCode = 'FINANCIAL_CLASS'
	LEFT JOIN HAppointment appt
		ON ord.VisitID = appt.VisitID
		AND ord.PatientID = appt.PatientID
		AND appt.ReferralID IS NOT NULL
	LEFT JOIN DMasterItem appt_req_status
		ON appt.AppointmentRequestStatusCode = appt_req_status.Code
		AND appt_req_status.CategoryCode = 'APPT_REQUEST_STATUS'
	LEFT JOIN DMasterItem appt_status
		ON appt.AppointmentStatusCode = appt_status.Code
		AND appt_status.CategoryCode = 'APPT_STATUS'
	LEFT JOIN DMasterItem appt_sched
		ON appt.SchedulingStatusCode = appt_sched.Code
		AND appt_sched.CategoryCode = 'SCHEDULING_STATUS'
WHERE
	1=1
	------------------------------------
	--@ORDER TYPE FILTER
	AND ISNULL(ord.OrderTypeCode, '') NOT IN
	(
		'MICROBIOLOGY'
		,'IMMUNIZATION'
		,'LAB'
		,'NURSING'
		,'OT'
		,'PT'
		,'RESPCARE'
		,'POC'
		,'PFT'
		,'IV'
		,'DIALYSIS'
		,'PATHOLOGY'
		,'DEVICECHECK'
		,'MEDICATION'
		,'OBULTRASOUND'
		,'SURGERYREQ'
		,'LABEXTERNAL'
	)
	------------------------------------
	--@ORDER CLASS FILTER
	AND ISNULL(ord.OrderClassCode, '') NOT IN
	(
		'NORMAL'
		,'POC'
		,'HISTORICAL'
		,'LABCOLLECT'
		,'OTC'
		,'PHONEIN'
		,'NOPRINT'
		,'UNITCOLLECT'
		,'PRINT'
		,'CLINICPERF'
		,'CLINICCOLLECT'
	)
	------------------------------------
	--@PROCEDURE CATEGORY FILTER
	AND ISNULL(cat.ProcedureCategoryCode, '') NOT IN
	(
		'NURSINGASSESS'
		,'NURSINGINFO'
		,'ADT'
		,'NURSINGTREAT_DISC'
		,'NURSINGTREAT_ONCE'
		,'DIET'
		,'SUPPLY'
		,'NURSINGEDU'
		,'PRECAUTION'
		,'NURSINGBLOOD'
		,'PHARMACONSULT'
		,'NURSINGMEDCOMM'
		,'NEONATETHERAPY'
	)
	------------------------------------
	--@ORDER EXPIRED FILTER
	AND (ord.ExpirationDateTime IS NULL OR CAST(ord.ExpirationDateTime AS DATE) >= CAST(GETDATE() AS DATE))
	------------------------------------
	--@NO-SHOW FILTER (known gap — captured but not filtered; enable once client confirms)
	-- AND ISNULL(appt.NoShowIndicator, 'N') <> 'Y'
	------------------------------------

-- =============================================================================================================
-- #SCHEDULE_INFO — Appointment and scheduling details (includes no-show and scheduling status)
-- =============================================================================================================

IF OBJECT_ID(N'TEMPDB..#SCHEDULE_INFO') IS NOT NULL
	DROP TABLE #SCHEDULE_INFO;

SELECT
	appt.AppointmentID
	,appt.PatientID
	,appt.VisitID
	,appt.ReferralID
	,CAST(appt.ScheduledDateTime AS DATE)               AS CONTACT_DATE
	,appt_status.Description                            AS APPT_STATUS
	,appt_sched_status.Description                      AS SCHEDULING_STATUS
	,appt.NoShowIndicator                               AS NO_SHOW_FLAG
	,appt.CheckInDateTime
	,appt.CheckOutDateTime
	,appt.CancelDateTime
	,cancel_reason.Description                          AS CANCEL_REASON

INTO #SCHEDULE_INFO

FROM
	HAppointment appt
	LEFT JOIN DMasterItem appt_status
		ON appt.AppointmentStatusCode = appt_status.Code
		AND appt_status.CategoryCode = 'APPT_STATUS'
	LEFT JOIN DMasterItem appt_sched_status
		ON appt.SchedulingStatusCode = appt_sched_status.Code
		AND appt_sched_status.CategoryCode = 'SCHEDULING_STATUS'
	LEFT JOIN DMasterItem cancel_reason
		ON appt.CancelReasonCode = cancel_reason.Code
		AND cancel_reason.CategoryCode = 'CANCEL_REASON'
WHERE
	appt.ReferralID IN
	(
	SELECT DISTINCT
		ReferralID
	FROM
		#REFERRAL_INFO
	)

-- =============================================================================================================
-- #VISIT_DX — Primary diagnosis per visit
-- =============================================================================================================

IF OBJECT_ID(N'TEMPDB..#VISIT_DX') IS NOT NULL
	DROP TABLE #VISIT_DX;

SELECT
	dx.VisitID
	,ref.ReferralID
	,ISNULL(dx.ICD10Code, dx.ICD9Code)                 AS DX_CODE
	,dx.DiagnosisDescription                            AS DX_NAME

INTO #VISIT_DX

FROM
	HPatientVisitDiagnosis dx
	INNER JOIN HPatientVisit visit ON dx.VisitID = visit.VisitID
	LEFT JOIN HReferral ref ON visit.VisitID = ref.VisitID
WHERE
	visit.VisitID IN
	(
	SELECT DISTINCT
		ORDER_VISIT_ID
	FROM
		#ORDER_INFO
	)
	AND dx.PrimaryDiagnosisIndicator = 'Y'

-- =============================================================================================================
-- #ORDER_CPT — Most recent CPT code per order catalog item
-- =============================================================================================================

IF OBJECT_ID(N'TEMPDB..#ORDER_CPT') IS NOT NULL
	DROP TABLE #ORDER_CPT;

SELECT *

INTO #ORDER_CPT

FROM
	(
	SELECT
		cat.OrderCatalogID                              AS PROC_ID
		,cat.CPTCode                                    AS CPT_CODE
		,cat.CPTDescription                             AS CPT_DESCRIPTION
		,cat.OrderName                                  AS NAME_HISTORY
		,cat.EffectiveDate                              AS CONTACT_DATE
		,ROW_NUMBER() OVER (
			PARTITION BY cat.OrderCatalogID
			ORDER BY cat.EffectiveDate DESC
		) AS RN
	FROM
		DOrderCatalog cat
	WHERE
		cat.OrderCatalogID IN
		(
		SELECT DISTINCT
			PROC_ID
		FROM
			#ORDER_INFO
		)
	) A
WHERE
	A.RN = 1

-- =============================================================================================================
-- #CVG_INFO — Insurance / coverage information
-- =============================================================================================================

IF OBJECT_ID(N'TEMPDB..#CVG_INFO') IS NOT NULL
	DROP TABLE #CVG_INFO;

SELECT DISTINCT
	ins.PlanName                                        AS PAT_BENEFIT_PLAN_NAME
	,ins_co.CompanyName                                 AS PAT_PAYOR_NAME
	,ins.SubscriberName                                 AS SUBSCR_NAME
	,ins.SubscriberID                                   AS SUBSCR_NUM
	,ins.GroupNumber                                    AS GROUP_NUM
	,ins.EffectiveDate                                  AS PAT_MEM_EFF_FROM_DATE
	,ins.ExpirationDate                                 AS PAT_MEM_EFF_TO_DATE
	,ins_co.AddressLine1                                AS BP_ADDR_LINE1
	,ins_co.AddressLine2                                AS BP_ADDR_LINE2
	,ins_co.City                                        AS BP_CITY
	,ins_co.State                                       AS BP_STATE
	,ins_co.ZipCode                                     AS BP_ZIP
	,ins_co.PhoneNumber                                 AS BP_PHONE
	,ins_co.FaxNumber                                   AS BP_FAX
	,ins.PatientInsuranceID                             AS PAT_COVERAGE_ID
	,ins.PatientID
	,ins.InsuranceCompanyID                             AS PAYOR_ID
	,ins.FilingOrder
	,ROW_NUMBER() OVER (
		PARTITION BY ins.PatientID, ins.PatientInsuranceID
		ORDER BY ins.ExpirationDate DESC, ins.FilingOrder ASC
	) AS RN

INTO #CVG_INFO

FROM
	HPatientInsurance ins
	LEFT JOIN HInsuranceCompany ins_co
		ON ins.InsuranceCompanyID = ins_co.InsuranceCompanyID
WHERE
	1=1
	AND
	(
		ins.ExpirationDate IS NULL
		OR
		CAST(ins.ExpirationDate AS DATE) >= DATEADD(DAY, -365, CAST(GETDATE() AS DATE))
	)
	AND ins.PatientInsuranceID IN
	(
	SELECT
		CVG_ID
	FROM
		#REFERRAL_INFO
	)

-- =============================================================================================================
-- FINAL SELECT — Consolidated output matching the Epic production query field set
-- Primary unique identifier: VisitID
-- =============================================================================================================

SELECT DISTINCT
	-- Patient
	#REFERRAL_INFO.PatientID                            AS 'patient_id'
	,#REFERRAL_INFO.VisitID                             AS 'visit_id'
	,#ORDER_INFO.ORDER_VISIT_ID                         AS 'encounter_visit_id'
	,#ORDER_INFO.HSP_ACCOUNT_ID                         AS 'account_number'
	,#REFERRAL_INFO.PAT_MRN                             AS 'patient_mrn'
	,#REFERRAL_INFO.PAT_FIRST_NAME                      AS 'patient_first_name'
	,#REFERRAL_INFO.PAT_LAST_NAME                       AS 'patient_last_name'
	,#REFERRAL_INFO.PAT_MIDDLE_NAME                     AS 'patient_middle_name'
	,#REFERRAL_INFO.SUFFIX                              AS 'patient_suffix'
	,#REFERRAL_INFO.ADD_LINE_1                          AS 'patient_address1'
	,#REFERRAL_INFO.ADD_LINE_2                          AS 'patient_address2'
	,#REFERRAL_INFO.CITY                                AS 'patient_city'
	,#REFERRAL_INFO.PATIENT_STATE                       AS 'patient_state'
	,#REFERRAL_INFO.ZIP                                 AS 'patient_zip'
	,#REFERRAL_INFO.PAT_DOB                             AS 'patient_birth_date'
	,#REFERRAL_INFO.SSN                                 AS 'patient_ssn'
	,#REFERRAL_INFO.PATIENT_CELL_PHONE                  AS 'patient_mobile_phone'
	,#REFERRAL_INFO.HOME_PHONE                          AS 'patient_home_phone'
	,#REFERRAL_INFO.WORK_PHONE                          AS 'patient_work_phone'
	,#REFERRAL_INFO.EMAIL_ADDRESS                       AS 'patient_email'
	,#REFERRAL_INFO.PATIENT_SEX                         AS 'patient_gender'
	,#REFERRAL_INFO.PATIENT_ETHNICITY                   AS 'patient_ethnicity'
	,#REFERRAL_INFO.PATIENT_MARITAL_STATUS              AS 'patient_marital_status'
	,#REFERRAL_INFO.PATIENT_RACE                        AS 'patient_primary_race'
	,#ORDER_INFO.PAT_CLASS                              AS 'patient_class'
	,#REFERRAL_INFO.PATIENT_LANGUAGE                    AS 'patient_language'
	,#REFERRAL_INFO.PAT_LIVING_STAT                     AS 'patient_deceased_flag'
	,#REFERRAL_INFO.SEND_SMS_YN                         AS 'patient_opt_out_sms'
	,#REFERRAL_INFO.NOTIF_PAT_EMAIL_YN                  AS 'patient_opt_out_email'
	,#REFERRAL_INFO.PAT_SPECIALTY                       AS 'specialty'
	-- Encounter
	,#ORDER_INFO.PAT_ENC_DEP                            AS 'encounter_department'
	,#ORDER_INFO.ENCOUNTER_CONTACT_DATE                 AS 'encounter_contact_date'
	-- Referral
	,#REFERRAL_INFO.ReferralID                          AS 'referral_id'
	,#REFERRAL_INFO.RFL_CLASS                           AS 'referral_class'
	,#REFERRAL_INFO.RFL_TYPE                            AS 'clinical_category'
	,#REFERRAL_INFO.REFERRAL_STATUS                     AS 'referral_status'
	,#REFERRAL_INFO.REF_STATUS_DETAIL                   AS 'referral_status_detail'
	,CAST(#REFERRAL_INFO.ReferralEntryDateTime AS DATE) AS 'referral_entry_date'
	,CAST(#REFERRAL_INFO.ReferralCloseDateTime AS DATE) AS 'referral_close_date'
	-- Order
	,#ORDER_INFO.OrderID                                AS 'order_id'
	,#ORDER_INFO.ORDER_TYPE                             AS 'order_type'
	,#ORDER_INFO.ORDERING_DATE                          AS 'order_date'
	,CAST(#ORDER_INFO.ORDER_INST AS DATE)               AS 'order_entry_date'
	,#ORDER_INFO.STANDING_EXP_DATE                      AS 'order_expiration_date'
	,#ORDER_INFO.PROC_NAME                              AS 'order_name'
	,#ORDER_INFO.AUTH_PROVIDER                          AS 'order_provider_name'
	,#ORDER_INFO.ORDER_PROV_NPI                         AS 'order_provider_npi'
	,#ORDER_INFO.ORDER_DISPLAY_NAME                     AS 'order_mnemonic'
	,#ORDER_INFO.PROC_CODE                              AS 'order_procedure'
	,#ORDER_INFO.DESCRIPTION                            AS 'order_description'
	,#ORDER_INFO.ORDER_STATUS                           AS 'order_status'
	,#ORDER_INFO.ORDER_RESULT_STATUS                    AS 'order_result_status'
	,DATEDIFF(DAY, #ORDER_INFO.ORDERING_DATE, GETDATE()) AS 'order_age'
	,#ORDER_INFO.AUTH_DEPT_NAME                         AS 'ordering_department_name'
	,#ORDER_INFO.ORDER_PROV_SPECIALTY                   AS 'ordering_provider_specialty'
	-- Procedure / Diagnosis
	,#ORDER_INFO.PROC_ID                                AS 'procedure_id'
	,#ORDER_INFO.PROC_CODE                              AS 'procedure_code'
	,#VISIT_DX.DX_CODE                                  AS 'dx_code'
	,#VISIT_DX.DX_NAME                                  AS 'dx_description'
	,#ORDER_CPT.CPT_CODE                                AS 'cpt_code'
	,#ORDER_CPT.CPT_DESCRIPTION                         AS 'cpt_procedure_description'
	,#ORDER_CPT.NAME_HISTORY                            AS 'cpt_description'
	,#ORDER_CPT.CONTACT_DATE                            AS 'max_procedure_contact_date'
	-- Appointment / Scheduling
	,#SCHEDULE_INFO.AppointmentID                       AS 'appointment_id'
	,#SCHEDULE_INFO.CONTACT_DATE                        AS 'appointment_date'
	,#SCHEDULE_INFO.APPT_STATUS                         AS 'appointment_status'
	,#ORDER_INFO.APPT_REQ_STATUS                        AS 'appointment_request_status'
	,#REFERRAL_INFO.SCHED_STATUS                        AS 'scheduling_status'
	,#ORDER_INFO.APPT_SCHED_STATUS                      AS 'appointment_scheduling_status'
	,#SCHEDULE_INFO.NO_SHOW_FLAG                        AS 'no_show_indicator'
	,#ORDER_INFO.NO_SHOW_FLAG                           AS 'order_no_show_indicator'
	-- Insurance
	,#ORDER_INFO.FINANCIAL_CLASS_NAME                   AS 'financial_class'
	,#CVG_INFO.PAT_BENEFIT_PLAN_NAME                    AS 'insurance_1_code'
	,#CVG_INFO.PAT_PAYOR_NAME                           AS 'insurance_1_payor_name'
	,#CVG_INFO.SUBSCR_NAME                              AS 'insurance_1_policy_holder'
	,#CVG_INFO.SUBSCR_NAME                              AS 'insurance_1_member_name'
	,#CVG_INFO.SUBSCR_NUM                               AS 'insurance_1_member_id'
	,#CVG_INFO.GROUP_NUM                                AS 'insurance_1_group_number'
	,#CVG_INFO.PAT_MEM_EFF_FROM_DATE                    AS 'insurance_1_effective_date'
	,#CVG_INFO.PAT_MEM_EFF_TO_DATE                      AS 'insurance_1_expiration_date'
	,#CVG_INFO.BP_ADDR_LINE1                            AS 'insurance_1_address1'
	,#CVG_INFO.BP_ADDR_LINE2                            AS 'insurance_1_address2'
	,#CVG_INFO.BP_CITY                                  AS 'insurance_1_city'
	,#CVG_INFO.BP_STATE                                 AS 'insurance_1_state'
	,#CVG_INFO.BP_ZIP                                   AS 'insurance_1_zip'
	,#CVG_INFO.BP_PHONE                                 AS 'insurance_1_phone'
	,#CVG_INFO.BP_FAX                                   AS 'insurance_1_fax'
	-- Facilities
	,#ORDER_INFO.REGION                                 AS 'region'
	,#ORDER_INFO.CAMPUS                                 AS 'campus'
	,#ORDER_INFO.LOC_NAME                               AS 'location'
	,#ORDER_INFO.POS_LOC_NAME                           AS 'referred_from_location'
	,#ORDER_INFO.LOC_ADD                                AS 'referred_from_location_address1'
	,#ORDER_INFO.LOC_CITY                               AS 'referred_from_location_city'
	,#ORDER_INFO.LOC_STATE                              AS 'referred_from_location_state'
	,#ORDER_INFO.LOC_ZIP                                AS 'referred_from_location_zip'
	,#ORDER_INFO.LOC_PHONE                              AS 'referred_from_location_phone_number'
	,#ORDER_INFO.LOC_FAX_NUM                            AS 'referred_from_location_fax_number'
	-- Referred To
	,#REFERRAL_INFO.REF_TO_PROV_NPI                     AS 'referred_to_provider_npi'
	,#REFERRAL_INFO.REF_TO_PROV                         AS 'referred_to_provider_name'
	,#REFERRAL_INFO.RSN_FOR_RFL                         AS 'referred_to_reason'
	,#REFERRAL_INFO.REF_TO_LOC_NAME                     AS 'referred_to_location'
	,#REFERRAL_INFO.REF_TO_ADDR_1                       AS 'referred_to_location_address'
	,#REFERRAL_INFO.REF_TO_LOC_CITY                     AS 'referred_to_location_city'
	,#REFERRAL_INFO.REF_TO_DEPT                         AS 'referred_to_department'
	,#REFERRAL_INFO.REF_TO_LOC_STATE                    AS 'referred_to_location_state'
	,#REFERRAL_INFO.REF_TO_LOC_ZIP                      AS 'referred_to_location_zip'
	,#REFERRAL_INFO.REF_TO_LOC_PHONE                    AS 'referred_to_location_phone_number'
FROM
	#REFERRAL_INFO
	INNER JOIN #ORDER_INFO ON #REFERRAL_INFO.OrderID = #ORDER_INFO.OrderID
	LEFT JOIN #SCHEDULE_INFO ON #REFERRAL_INFO.ReferralID = #SCHEDULE_INFO.ReferralID
	LEFT JOIN #VISIT_DX ON #ORDER_INFO.ORDER_VISIT_ID = #VISIT_DX.VisitID
	LEFT JOIN #ORDER_CPT ON #ORDER_INFO.PROC_ID = #ORDER_CPT.PROC_ID
	LEFT JOIN #CVG_INFO
		ON #REFERRAL_INFO.CVG_ID = #CVG_INFO.PAT_COVERAGE_ID
		AND #CVG_INFO.PatientID = #REFERRAL_INFO.PatientID
		AND #CVG_INFO.RN = 1
		AND
		(
			(#CVG_INFO.PAT_MEM_EFF_FROM_DATE <= #ORDER_INFO.ORDERING_DATE AND #CVG_INFO.PAT_MEM_EFF_TO_DATE >= #ORDER_INFO.ORDERING_DATE)
			OR (#CVG_INFO.PAT_MEM_EFF_FROM_DATE <= #ORDER_INFO.ORDERING_DATE AND #CVG_INFO.PAT_MEM_EFF_TO_DATE IS NULL)
		)
WHERE
	1=1
	AND #SCHEDULE_INFO.CONTACT_DATE IS NULL
ORDER BY
	#REFERRAL_INFO.VisitID, #REFERRAL_INFO.PatientID

-- =============================================================================================================
-- Cleanup
-- =============================================================================================================

DROP TABLE #REFERRAL_INFO;
DROP TABLE #ORDER_INFO;
DROP TABLE #SCHEDULE_INFO;
DROP TABLE #VISIT_DX;
DROP TABLE #ORDER_CPT;
DROP TABLE #CVG_INFO;
