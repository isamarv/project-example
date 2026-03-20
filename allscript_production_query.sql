/*
-----------------------------------------------------------------------------------------------------------------
-- Abax Healthcare, Inc — ALLSCRIPTS/ALTERA SUNRISE DATA EXTRACTION QUERY
-- Contact: Jay Dalke
-- Modified: 03/20/2026
-- Email: support@abaxhealth.com
-- Please reach out to Jay Dalke for any assistance with this query.
-- Query support is part of Abax Health's comprehensive implementation process.
-----------------------------------------------------------------------------------------------------------------
-- Platform:      Allscripts/Altera Sunrise Clinical Manager (SCM) — Hosted Environment
-- Database:      Microsoft SQL Server (T-SQL)
-- Primary Key:   VisitID (HPatientVisit.VisitID)
-- Source Script: Converted from Epic Clarity production query
-----------------------------------------------------------------------------------------------------------------
-- Purpose:
-- This query extracts detailed patient, order, referral, and scheduling information for Abax Health
-- from an Allscripts/Altera Sunrise Clinical Manager database.
-- It utilizes temporary tables and joins multiple data sources for comprehensive reporting.
-- All output fields are mapped 1:1 from the Epic Clarity production query.
-----------------------------------------------------------------------------------------------------------------
-- Filters:
-- @DATE FILTER
--      Captures all referrals from the past 365 days.
--      The filter retrieves referral records within the 365-day timeframe.
--      This ensures that Abax Health receives updated information on referrals captured within this period.
-- @DECEASED FILTER
--      Excludes deceased patients from results returned.
-- @NO-SHOW FILTER
--      ** KNOWN GAP: The no-show filter is not being used properly in this environment. **
--      The filter is included for data capture and completeness. It may require further
--      adjustment after initial data review with the client.
-- @ORDER TYPE FILTER
--      Excludes order types not in scope of work.
--      Dictionary values must be validated against the client Sunrise instance.
-- @SCHEDULING STATUS FILTER
--      Captures appointment status and scheduling status per client requirements.
--      Both appointment-level status and referral-level scheduling status are included.
-----------------------------------------------------------------------------------------------------------------
-- Environment Notes:
-- 1. Table names follow the standard Sunrise SCM naming convention.
--    If the hosted environment uses a different schema or prefix, adjust table names accordingly.
-- 2. HDictionary is the universal lookup table in Allscripts Sunrise, replacing all Epic ZC_* tables.
--    Dictionary values are resolved by joining on the DictionaryID foreign key columns.
--    Validate DictionaryID values and category names against the client data dictionary.
-- 3. VisitID (HPatientVisit.VisitID) is the primary unique identifier per client specification.
-- 4. Hosted environments may require schema prefixes (e.g., dbo.) — adjust as needed.
-- 5. Column names and FK relationships may vary by Sunrise version and configuration.
--    The script includes comments where adjustments are most likely needed.
-----------------------------------------------------------------------------------------------------------------
-- Key Table Mappings (Epic Clarity → Allscripts Sunrise):
--   PATIENT / PATIENT_3 / PATIENT_4     → HPerson + HPatient + HPatientExtension
--   PAT_ENC                              → HPatientVisit
--   ORDER_PROC / ORDER_PROC_2            → HOrder
--   REFERRAL / REFERRAL_ORDER_ID         → HReferral + HReferralOrder
--   REFERRAL_CVG                         → HPatientInsurance (linked via referral)
--   CLARITY_SER / CLARITY_SER_2          → HStaff
--   CLARITY_SER_SPEC                     → HStaffSpecialty
--   CLARITY_EAP                          → HOrderCatalogMasterItem
--   CLARITY_EAP_OT                       → HOrderCatalogCPT
--   CLARITY_DEP                          → HDepartment
--   CLARITY_LOC / CLARITY_POS            → HFacility + HLocation
--   F_SCHED_APPT / ZC_APPT_STATUS        → HAppointment + HDictionary
--   APPT_REQUEST / ZC_APPT_REQ_STATUS     → HAppointment (appointment request status)
--   ZC_* (all lookup tables)              → HDictionary
--   COVERAGE / V_COVERAGE_PAYOR_PLAN      → HPatientInsurance + HInsuranceCompany + HInsurancePlan
--   COVERAGE_MEM_LIST / PAT_CVG_FILE_ORDER → HPatientInsurance (filing order + effective dates)
--   PAT_ENC_DX / CLARITY_EDG             → HPatientVisitDiagnosis + HDiagnosis
--   CLARITY_EPM / CLARITY_EPP            → HInsuranceCompany + HInsurancePlan
--   CLARITY_FC                           → HDictionary (financial class)
--   OTHER_COMMUNCTN                      → HPersonPhone (contact type filtered)
--   PATIENT_RACE                         → HPersonRace
--   ZC_SEX / ZC_STATE / ZC_SUFFIX etc.   → HDictionary (single universal lookup)
--   ZC_SCHED_STATUS                      → HDictionary (scheduling status on referral)
--   ZC_LOC_RPT_GRP_6 / _7               → HFacility region/campus attributes
-----------------------------------------------------------------------------------------------------------------
*/


-- =============================================================================================================
-- TEMP TABLE 1: REFERRAL INFORMATION
-- Captures referral records joined with patient demographics, coverage, and referred-to details.
-- Epic equivalent: #REFERRAL_INFO
-- =============================================================================================================

IF OBJECT_ID(N'TEMPDB..#REFERRAL_INFO') IS NOT NULL
	DROP TABLE #REFERRAL_INFO;

SELECT
	ref.ReferralID
	,ref.EntryDateTime                                          AS ReferralEntryDate
	,ref.CloseDateTime                                          AS ReferralCloseDate
	,COALESCE(ro.OrderID, ref.OrderID)                          AS OrderID
	,ref.PatientID
	,ref.VisitID                                                AS ReferralVisitID
	,pat.MRN                                                    AS PatientMRN
	,ic.Name                                                    AS PrimaryPayor
	,dict_rfl_status.DisplayName                                AS ReferralStatus
	,dict_sched_status.DisplayName                              AS SchedStatus
	,per.FirstName                                              AS PatFirstName
	,per.LastName                                               AS PatLastName
	,per.MiddleName                                             AS PatMiddleName
	,dict_suffix.DisplayName                                    AS Suffix
	,addr.AddressLine1
	,addr.AddressLine2
	,addr.City
	,dict_addr_state.DisplayName                                AS PatientState
	,addr.PostalCode                                            AS Zip
	,CAST(per.DateOfBirth AS DATE)                              AS PatDOB
	,per.SSN
	,cellphone.PhoneNumber                                      AS PatientCellPhone
	,homephone.PhoneNumber                                      AS HomePhone
	,workphone.PhoneNumber                                      AS WorkPhone
	,per.EmailAddress
	,dict_sex.DisplayName                                       AS PatientSex
	,dict_ethnicity.DisplayName                                 AS PatientEthnicity
	,dict_marital.DisplayName                                   AS PatientMaritalStatus
	,dict_race.DisplayName                                      AS PatientRace
	,dict_language.DisplayName                                  AS PatientLanguage
	,ref_to_staff.Name                                          AS RefToProv
	,ref_to_staff.NPI                                           AS RefToProvNPI
	,dict_reason.DisplayName                                    AS RsnForRfl
	,pi.PatientInsuranceID                                      AS CvgID
	,ISNULL(per.IsDeceased, 0)                                  AS PatLivingStatC
	,patpref.OptOutSMS                                          AS SendSmsYN
	,patpref.OptOutEmail                                        AS NotifPatEmailYN
	,dict_rfl_class.DisplayName                                 AS RflClass
	,dict_rfl_type.DisplayName                                  AS RflType
	,dict_ref_detail.DisplayName                                AS RefStatusDetail
	,dict_pat_specialty.DisplayName                             AS PatSpecialty
	,ref_fac.Name                                               AS RefToLocName
	,ref_fac.AddressLine1                                       AS RefToAddr1
	,ref_fac.City                                               AS RefToLocCity
	,ref_dept.Name                                              AS RefToDept
	,dict_ref_fac_state.DisplayName                             AS RefToLocState
	,ref_fac.PostalCode                                         AS RefToLocZip
	,ref_fac.AreaCode                                           AS RefToLocAreaCode
	,ref_fac.PhoneNumber                                        AS RefToLocPhone

INTO #REFERRAL_INFO

FROM
	HReferral ref
	-- Link referral to order (junction table or direct FK — validate for your environment)
	LEFT JOIN HReferralOrder ro
		ON ref.ReferralID = ro.ReferralID
	-- Patient
	LEFT JOIN HPatient pat
		ON ref.PatientID = pat.PatientID
	LEFT JOIN HPerson per
		ON pat.PatientID = per.PersonID
	-- Patient Address (primary/home)
	-- NOTE: AddressTypeID filter value must match your HDictionary for "Home" address type.
	LEFT JOIN HPersonAddress addr
		ON per.PersonID = addr.PersonID
		AND addr.IsActive = 1
		AND addr.IsPrimary = 1
	-- Patient Phone Numbers
	-- NOTE: PhoneTypeID values must match your HDictionary entries for Cell, Home, Work.
	LEFT JOIN HPersonPhone cellphone
		ON per.PersonID = cellphone.PersonID
		AND cellphone.PhoneTypeID = (SELECT TOP 1 DictionaryID FROM HDictionary WHERE DisplayName = 'Cell' AND Category = 'PhoneType')
	LEFT JOIN HPersonPhone homephone
		ON per.PersonID = homephone.PersonID
		AND homephone.PhoneTypeID = (SELECT TOP 1 DictionaryID FROM HDictionary WHERE DisplayName = 'Home' AND Category = 'PhoneType')
	LEFT JOIN HPersonPhone workphone
		ON per.PersonID = workphone.PersonID
		AND workphone.PhoneTypeID = (SELECT TOP 1 DictionaryID FROM HDictionary WHERE DisplayName = 'Work' AND Category = 'PhoneType')
	-- Patient Preferences (SMS/Email opt-out)
	LEFT JOIN HPatientPreference patpref
		ON pat.PatientID = patpref.PatientID
	-- Insurance (primary coverage — filing order 1)
	LEFT JOIN HPatientInsurance pi
		ON pat.PatientID = pi.PatientID
		AND pi.FilingOrder = 1
		AND (pi.ExpirationDate IS NULL OR pi.ExpirationDate >= CAST(GETDATE() AS DATE))
	LEFT JOIN HInsuranceCompany ic
		ON pi.InsuranceCompanyID = ic.InsuranceCompanyID
	-- Referred-To Provider
	LEFT JOIN HStaff ref_to_staff
		ON ref.ReferredToStaffID = ref_to_staff.StaffID
	-- Referred-To Location / Department
	LEFT JOIN HFacility ref_fac
		ON ref.ReferredToFacilityID = ref_fac.FacilityID
	LEFT JOIN HDepartment ref_dept
		ON ref.ReferredToDepartmentID = ref_dept.DepartmentID
	-- Patient Specialty (from patient's primary department)
	LEFT JOIN HDepartment pat_dept
		ON pat.PrimaryDepartmentID = pat_dept.DepartmentID
	-- Race (primary race, first entry)
	LEFT JOIN HPersonRace prace
		ON per.PersonID = prace.PersonID
		AND prace.Sequence = 1
	-- Dictionary Lookups (Allscripts HDictionary replaces all Epic ZC_* tables)
	LEFT JOIN HDictionary dict_sex
		ON per.SexID = dict_sex.DictionaryID
	LEFT JOIN HDictionary dict_addr_state
		ON addr.StateID = dict_addr_state.DictionaryID
	LEFT JOIN HDictionary dict_suffix
		ON per.SuffixID = dict_suffix.DictionaryID
	LEFT JOIN HDictionary dict_ethnicity
		ON per.EthnicityID = dict_ethnicity.DictionaryID
	LEFT JOIN HDictionary dict_marital
		ON per.MaritalStatusID = dict_marital.DictionaryID
	LEFT JOIN HDictionary dict_language
		ON per.PreferredLanguageID = dict_language.DictionaryID
	LEFT JOIN HDictionary dict_race
		ON prace.RaceID = dict_race.DictionaryID
	LEFT JOIN HDictionary dict_rfl_status
		ON ref.ReferralStatusID = dict_rfl_status.DictionaryID
	LEFT JOIN HDictionary dict_sched_status
		ON ref.SchedulingStatusID = dict_sched_status.DictionaryID
	LEFT JOIN HDictionary dict_rfl_class
		ON ref.ReferralClassID = dict_rfl_class.DictionaryID
	LEFT JOIN HDictionary dict_rfl_type
		ON ref.ReferralTypeID = dict_rfl_type.DictionaryID
	LEFT JOIN HDictionary dict_ref_detail
		ON ref.ReferralDetailStatusID = dict_ref_detail.DictionaryID
	LEFT JOIN HDictionary dict_reason
		ON ref.ReasonForReferralID = dict_reason.DictionaryID
	LEFT JOIN HDictionary dict_ref_fac_state
		ON ref_fac.StateID = dict_ref_fac_state.DictionaryID
	LEFT JOIN HDictionary dict_pat_specialty
		ON pat_dept.SpecialtyID = dict_pat_specialty.DictionaryID
WHERE
	1=1
	--@DATE FILTER
	AND CAST(ref.EntryDateTime AS DATE) >= DATEADD(DAY, -365, CAST(GETDATE() AS DATE))
	------------------------------------
	--@DECEASED FILTER
	AND ISNULL(per.IsDeceased, 0) <> 1
	------------------------------------
	--@SCHEDULING STATUS FILTER
	-- NOTE: Adjust these values to match your Sunrise dictionary entries.
	-- These are carried over from the Epic Clarity equivalent and may differ in Sunrise.
	AND ISNULL(dict_sched_status.DisplayName, '') NOT IN
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
----------------------------------------------------------------------------------------------------------


-- =============================================================================================================
-- TEMP TABLE 2: ORDER INFORMATION
-- Captures order details with provider, department, location, and financial class data.
-- Epic equivalent: #ORDER_INFO
-- =============================================================================================================

IF OBJECT_ID(N'TEMPDB..#ORDER_INFO') IS NOT NULL
	DROP TABLE #ORDER_INFO;

SELECT
	ord.OrderID                                                 AS OrderProcID
	,CAST(ord.OrderDateTime AS DATE)                            AS OrderingDate
	,ord.EntryDateTime                                          AS OrderInst
	,ord.Description                                            AS Description
	,ord.VisitID                                                AS OrderCSN
	,ord.PatientID
	,dict_ord_status.DisplayName                                AS OrderStatus
	,dict_ord_result.DisplayName                                AS OrderResultStatus
	,ocmi.Code                                                  AS ProcCode
	,ocmi.Name                                                  AS ProcName
	,ocmi.MnemonicName                                          AS OrderDisplayName
	,ord_staff.Name                                             AS AuthProvider
	,ord_staff.NPI                                              AS OrderProvNPI
	,dict_ord_spec.DisplayName                                  AS OrderProvSpecialty
	,dict_ord_type.DisplayName                                  AS OrderType
	,ord_dept.Name                                              AS DepartmentName
	,auth_dept.Name                                             AS AuthDeptName
	,ord_fac.Name                                               AS LocName
	,dict_region.DisplayName                                    AS Region
	,dict_campus.DisplayName                                    AS Campus
	,ord.OrderCatalogMasterItemID                               AS ProcID
	,ord.ExpirationDateTime                                     AS StandingExpDate
	,pv.AccountNumber                                           AS HspAccountID
	,pv.AdmitDateTime                                           AS EncounterContactDate
	,dict_pat_class.DisplayName                                 AS PatClass
	,enc_dept.Name                                              AS PatEncDep
	,enc_fac.Name                                               AS PosLocName
	,enc_fac.AddressLine1                                       AS LocAdd
	,enc_fac.AddressLine2                                       AS LocAdd2
	,enc_fac.City                                               AS LocCity
	,dict_enc_fac_state.DisplayName                             AS LocState
	,enc_fac.PostalCode                                         AS LocZip
	,enc_fac.EmergencyPhone                                     AS LocEmergPhone
	,enc_fac.FaxNumber                                          AS LocFaxNum
	,dict_fin_class.DisplayName                                 AS FinancialClassName
	,dict_appt_status.DisplayName                               AS ApptReqStatus

INTO #ORDER_INFO

FROM
	HOrder ord
	LEFT JOIN HOrderCatalogMasterItem ocmi
		ON ord.OrderCatalogMasterItemID = ocmi.OrderCatalogMasterItemID
	-- Ordering / Authorizing Provider
	LEFT JOIN HStaff ord_staff
		ON ord.OrderingStaffID = ord_staff.StaffID
	LEFT JOIN HStaffSpecialty ord_staff_spec
		ON ord_staff.StaffID = ord_staff_spec.StaffID
		AND ord_staff_spec.IsPrimary = 1
	-- Department and Location for the order
	LEFT JOIN HDepartment ord_dept
		ON ord.DepartmentID = ord_dept.DepartmentID
	LEFT JOIN HDepartment auth_dept
		ON ord.AuthorizingDepartmentID = auth_dept.DepartmentID
	LEFT JOIN HFacility ord_fac
		ON ord_dept.FacilityID = ord_fac.FacilityID
	-- Visit / Encounter
	LEFT JOIN HPatientVisit pv
		ON ord.VisitID = pv.VisitID
	LEFT JOIN HDepartment enc_dept
		ON pv.DepartmentID = enc_dept.DepartmentID
	LEFT JOIN HFacility enc_fac
		ON enc_dept.FacilityID = enc_fac.FacilityID
	-- Appointment linked to order (appointment request status)
	LEFT JOIN HAppointment appt_req
		ON ord.OrderID = appt_req.OrderID
	-- Financial Class
	-- NOTE: FinancialClassID may live on HPatientVisit or HOrder depending on configuration.
	LEFT JOIN HDictionary dict_fin_class
		ON pv.FinancialClassID = dict_fin_class.DictionaryID
	-- Dictionary Lookups
	LEFT JOIN HDictionary dict_ord_status
		ON ord.OrderStatusID = dict_ord_status.DictionaryID
	LEFT JOIN HDictionary dict_ord_result
		ON ord.OrderResultStatusID = dict_ord_result.DictionaryID
	LEFT JOIN HDictionary dict_ord_type
		ON ord.OrderTypeID = dict_ord_type.DictionaryID
	LEFT JOIN HDictionary dict_ord_spec
		ON ord_staff_spec.SpecialtyID = dict_ord_spec.DictionaryID
	LEFT JOIN HDictionary dict_pat_class
		ON pv.PatientClassID = dict_pat_class.DictionaryID
	LEFT JOIN HDictionary dict_enc_fac_state
		ON enc_fac.StateID = dict_enc_fac_state.DictionaryID
	LEFT JOIN HDictionary dict_region
		ON ord_fac.RegionID = dict_region.DictionaryID
	LEFT JOIN HDictionary dict_campus
		ON ord_fac.CampusID = dict_campus.DictionaryID
	LEFT JOIN HDictionary dict_appt_status
		ON appt_req.AppointmentStatusID = dict_appt_status.DictionaryID
WHERE
	1=1
	--@ORDER TYPE FILTER
	-- NOTE: Validate these OrderTypeID values against your Sunrise HDictionary.
	-- The Epic equivalents are listed in comments. Replace with actual DictionaryID values.
	AND ISNULL(dict_ord_type.DisplayName, '') NOT IN
	(
		'Micro Biology'             -- Epic ORDER_TYPE_C = 3
		,'Immunizations'            -- Epic ORDER_TYPE_C = 6
		,'Lab'                      -- Epic ORDER_TYPE_C = 7
		,'Nursing'                  -- Epic ORDER_TYPE_C = 10
		,'OT'                       -- Epic ORDER_TYPE_C = 16
		,'PT'                       -- Epic ORDER_TYPE_C = 17
		,'Respiratory Care'         -- Epic ORDER_TYPE_C = 18
		,'Point of Care'            -- Epic ORDER_TYPE_C = 26 / 63
		,'PFT'                      -- Epic ORDER_TYPE_C = 35
		,'IV'                       -- Epic ORDER_TYPE_C = 43
		,'Dialysis'                 -- Epic ORDER_TYPE_C = 46
		,'Pathology'                -- Epic ORDER_TYPE_C = 59
		,'Device Check'             -- Epic ORDER_TYPE_C = 105
		,'Medications'              -- Epic ORDER_TYPE_C = 999
		,'OB Ultra Sound'           -- Epic ORDER_TYPE_C = 1001
		,'Surgery Request'          -- Epic ORDER_TYPE_C = 1002
		,'Lab External'             -- Epic ORDER_TYPE_C = 1007
	)
	---------------------------------------------------------------------------------------------------------------
	--@ORDER CLASS FILTER
	-- NOTE: Validate these against your Sunrise dictionary entries for Order Class.
	AND ISNULL(ord.OrderClassCode, '') NOT IN
	(
		'Normal'
		,'Point of Care'
		,'Historical'
		,'Lab Collect'
		,'OTC'
		,'Phone In'
		,'No Print'
		,'Unit Collect'
		,'Print'
		,'Clinic Performed'
		,'Clinic Collect'
	)
	---------------------------------------------------------------------------------------------------------------
	--@PROCEDURE CATEGORY FILTER
	-- NOTE: Validate CategoryID values against your Sunrise HOrderCatalogMasterItem categories.
	AND ISNULL(ocmi.CategoryCode, '') NOT IN
	(
		'Nursing Assessment Orderables - Once or at Intervals'
		,'Nursing Informational/Communication Orderables'
		,'ADT Orderables'
		,'Nursing Treatment Orderables - Until Discontinued'
		,'Nursing Treatment Orderables - Once or at Intervals'
		,'Diet Orderables'
		,'General Supply & Equipment Orderables'
		,'Nursing Education Orderables'
		,'Precaution Orderables'
		,'Nursing Treatment Orderables - Blood Admin'
		,'Pharmacy Consult Orderables'
		,'Nursing Med Communication'
		,'Neonate Therapies'
	)
	---------------------------------------------------------------------------------------------------------------
	--@ORDER EXPIRED FILTER
	AND (ord.ExpirationDateTime IS NULL OR CAST(ord.ExpirationDateTime AS DATE) >= CAST(GETDATE() AS DATE))
----------------------------------------------------------------------------------------------------------


-- =============================================================================================================
-- TEMP TABLE 3: SCHEDULE / APPOINTMENT INFORMATION
-- Captures appointment records linked to referrals, including appointment and scheduling status.
-- Epic equivalent: #SCHEDULE_INFO
-- =============================================================================================================

IF OBJECT_ID(N'TEMPDB..#SCHEDULE_INFO') IS NOT NULL
	DROP TABLE #SCHEDULE_INFO;

SELECT
	appt.AppointmentID
	,appt.VisitID
	,appt.PatientID
	,appt.ReferralID
	,CAST(appt.AppointmentDateTime AS DATE)                     AS ContactDate
	,appt.ScheduleEventID
	,appt.LocationID
	,appt.DepartmentID
	,appt.ProviderStaffID
	,dict_appt_status.DisplayName                               AS ApptStatus
	,dict_appt_sched_status.DisplayName                         AS ApptSchedStatus
	-- ** NO-SHOW FLAG **
	-- KNOWN GAP: The no-show filter is not being used properly in this environment.
	-- This flag is captured for reporting and future remediation.
	,CASE
		WHEN dict_appt_status.DisplayName = 'No Show' THEN 1
		ELSE 0
	 END                                                        AS NoShowFlag

INTO #SCHEDULE_INFO

FROM
	HAppointment appt
	LEFT JOIN HDictionary dict_appt_status
		ON appt.AppointmentStatusID = dict_appt_status.DictionaryID
	-- Scheduling status at the appointment level (if tracked separately from appointment status)
	LEFT JOIN HDictionary dict_appt_sched_status
		ON appt.SchedulingStatusID = dict_appt_sched_status.DictionaryID
WHERE
	appt.ReferralID IN
	(
	SELECT DISTINCT
		ReferralID
	FROM
		#REFERRAL_INFO
	)

----------------------------------------------------------------------------------------------------------


-- =============================================================================================================
-- TEMP TABLE 4: VISIT DIAGNOSES
-- Captures primary diagnosis for each encounter linked to orders.
-- Epic equivalent: #PAT_ENC_DX
-- =============================================================================================================

IF OBJECT_ID(N'TEMPDB..#VISIT_DIAGNOSIS') IS NOT NULL
	DROP TABLE #VISIT_DIAGNOSIS;

SELECT
	pvd.VisitID
	,ref.ReferralID
	,COALESCE(dx.ICD10Code, dx.ICD9Code)                       AS DxCode
	,dx.Description                                             AS DxName

INTO #VISIT_DIAGNOSIS

FROM
	HPatientVisitDiagnosis pvd
	INNER JOIN HPatientVisit pv
		ON pvd.VisitID = pv.VisitID
	LEFT JOIN HDiagnosis dx
		ON pvd.DiagnosisID = dx.DiagnosisID
	LEFT JOIN HReferral ref
		ON pv.VisitID = ref.VisitID
WHERE
	pv.VisitID IN
	(
	SELECT DISTINCT
		OrderCSN
	FROM
		#ORDER_INFO
	)
	AND pvd.IsPrimary = 1

----------------------------------------------------------------------------------------------------------


-- =============================================================================================================
-- TEMP TABLE 5: ORDER CPT CODES
-- Captures the most recent CPT code mapping for each order catalog item.
-- Epic equivalent: #ORDER_PROC_CPT
-- =============================================================================================================

IF OBJECT_ID(N'TEMPDB..#ORDER_CPT') IS NOT NULL
	DROP TABLE #ORDER_CPT;

SELECT *

INTO #ORDER_CPT

FROM
	(
	SELECT
		cpt.OrderCatalogMasterItemID                            AS ProcID
		,cpt.EffectiveDate                                      AS ContactDate
		,cpt.CPTCode
		,cpt.Description
		,cpt.NameHistory
		,ROW_NUMBER() OVER (PARTITION BY cpt.OrderCatalogMasterItemID ORDER BY cpt.EffectiveDate DESC) AS RN
	FROM
		HOrderCatalogCPT cpt
	WHERE
		cpt.OrderCatalogMasterItemID IN
		(
		SELECT DISTINCT
			ProcID
		FROM
			#ORDER_INFO
		)
	) A
WHERE
	A.RN = 1

----------------------------------------------------------------------------------------------------------


-- =============================================================================================================
-- TEMP TABLE 6: INSURANCE / COVERAGE INFORMATION
-- Captures patient insurance details including plan, payor, subscriber, and benefit plan address.
-- Epic equivalent: #CVG_INFO
-- =============================================================================================================

IF OBJECT_ID(N'TEMPDB..#INSURANCE_INFO') IS NOT NULL
	DROP TABLE #INSURANCE_INFO;

SELECT DISTINCT
	ip.PlanName                                                 AS PatBenefitPlanName
	,ic.Name                                                    AS PatPayorName
	,pi.SubscriberName                                          AS SubscrName
	,pi.MemberID                                                AS SubscrNum
	,pi.GroupNumber                                             AS GroupNum
	,pi.EffectiveDate                                           AS PatMemEffFromDate
	,pi.ExpirationDate                                          AS PatMemEffToDate
	,ic.AddressLine1                                            AS BpAddrLine1
	,ic.AddressLine2                                            AS BpAddrLine2
	,ic.City                                                    AS BpCity
	,dict_ic_state.DisplayName                                  AS BpState
	,ic.PostalCode                                              AS BpZip
	,ic.PhoneNumber                                             AS BpPhone
	,ic.FaxNumber                                               AS BpFax
	,pi.PatientInsuranceID                                      AS PatCoverageID
	,pi.PatientID
	,pi.InsuranceCompanyID                                      AS PayorID
	,ROW_NUMBER() OVER (
		PARTITION BY pi.PatientID, pi.PatientInsuranceID
		ORDER BY pi.ExpirationDate DESC, pi.FilingOrder ASC
	)                                                           AS RN

INTO #INSURANCE_INFO

FROM
	HPatientInsurance pi
	LEFT JOIN HInsurancePlan ip
		ON pi.InsurancePlanID = ip.InsurancePlanID
	LEFT JOIN HInsuranceCompany ic
		ON pi.InsuranceCompanyID = ic.InsuranceCompanyID
	LEFT JOIN HDictionary dict_ic_state
		ON ic.StateID = dict_ic_state.DictionaryID
WHERE
	1=1
	AND
	(
		pi.ExpirationDate IS NULL
		OR
		CAST(pi.ExpirationDate AS DATE) >= DATEADD(DAY, -365, CAST(GETDATE() AS DATE))
	)
	AND pi.PatientInsuranceID IN
	(
	SELECT
		CvgID
	FROM
		#REFERRAL_INFO
	)

----------------------------------------------------------------------------------------------------------


-- =============================================================================================================
-- FINAL SELECT
-- Joins all temp tables to produce the comprehensive output dataset.
-- Output field aliases match the Epic production query for consistent downstream mapping.
-- Primary unique identifier: VisitID
-- =============================================================================================================

SELECT DISTINCT
	-- Patient
	#REFERRAL_INFO.PatientID AS                                 'patient_id'
	,#REFERRAL_INFO.ReferralVisitID AS                          'visit_id'
	,#ORDER_INFO.OrderCSN AS                                    'encounter_csn_id'
	,#ORDER_INFO.HspAccountID AS                                'account_number'
	,#REFERRAL_INFO.PatientMRN AS                               'patient_mrn'
	,#REFERRAL_INFO.PatFirstName AS                             'patient_first_name'
	,#REFERRAL_INFO.PatLastName AS                              'patient_last_name'
	,#REFERRAL_INFO.PatMiddleName AS                            'patient_middle_name'
	,#REFERRAL_INFO.Suffix AS                                   'patient_suffix'
	,#REFERRAL_INFO.AddressLine1 AS                             'patient_address1'
	,#REFERRAL_INFO.AddressLine2 AS                             'patient_address2'
	,#REFERRAL_INFO.City AS                                     'patient_city'
	,#REFERRAL_INFO.PatientState AS                             'patient_state'
	,#REFERRAL_INFO.Zip AS                                      'patient_zip'
	,#REFERRAL_INFO.PatDOB AS                                   'patient_birth_date'
	,#REFERRAL_INFO.SSN AS                                      'patient_ssn'
	,#REFERRAL_INFO.PatientCellPhone AS                         'patient_mobile_phone'
	,#REFERRAL_INFO.HomePhone AS                                'patient_home_phone'
	,#REFERRAL_INFO.WorkPhone AS                                'patient_work_phone'
	,#REFERRAL_INFO.EmailAddress AS                             'patient_email'
	,#REFERRAL_INFO.PatientSex AS                               'patient_gender'
	,#REFERRAL_INFO.PatientEthnicity AS                         'patient_ethnicity'
	,#REFERRAL_INFO.PatientMaritalStatus AS                     'patient_marital_status'
	,#REFERRAL_INFO.PatientRace AS                              'patient_primary_race'
	,#ORDER_INFO.PatClass AS                                    'patient_class'
	,#REFERRAL_INFO.PatientLanguage AS                          'patient_language'
	,#REFERRAL_INFO.PatLivingStatC AS                           'patient_deceased_flag'
	,#REFERRAL_INFO.SendSmsYN AS                                'patient_opt_out_sms'
	,#REFERRAL_INFO.NotifPatEmailYN AS                          'patient_opt_out_email'
	,#REFERRAL_INFO.PatSpecialty AS                              'specialty'
	-- Encounter
	,#ORDER_INFO.PatEncDep AS                                   'encounter_department'
	,CAST(#ORDER_INFO.EncounterContactDate AS DATE) AS          'encounter_contact_date'
	-- Referral
	,#REFERRAL_INFO.ReferralID AS                               'referral_id'
	,#REFERRAL_INFO.RflClass AS                                 'referral_class'
	,#REFERRAL_INFO.RflType AS                                  'clinical_category'
	,#REFERRAL_INFO.ReferralStatus AS                           'referral_status'
	,#REFERRAL_INFO.RefStatusDetail AS                          'referral_status_detail'
	,CAST(#REFERRAL_INFO.ReferralEntryDate AS DATE) AS          'referral_entry_date'
	,CAST(#REFERRAL_INFO.ReferralCloseDate AS DATE) AS          'referral_close_date'
	-- Order
	,#ORDER_INFO.OrderProcID AS                                 'order_id'
	,#ORDER_INFO.OrderType AS                                   'order_type'
	,CAST(#ORDER_INFO.OrderingDate AS DATE) AS                  'order_date'
	,CAST(#ORDER_INFO.OrderInst AS DATE) AS                     'order_entry_date'
	,#ORDER_INFO.StandingExpDate AS                             'order_expiration_date'
	,#ORDER_INFO.ProcName AS                                    'order_name'
	,#ORDER_INFO.AuthProvider AS                                'order_provider_name'
	,#ORDER_INFO.OrderProvNPI AS                                'order_provider_npi'
	,#ORDER_INFO.OrderDisplayName AS                            'order_mnemonic'
	,#ORDER_INFO.ProcCode AS                                    'order_procedure'
	,#ORDER_INFO.Description AS                                 'order_description'
	,#ORDER_INFO.OrderStatus AS                                 'order_status'
	,#ORDER_INFO.OrderResultStatus AS                           'order_result_status'
	,DATEDIFF(DAY, #ORDER_INFO.OrderingDate, GETDATE()) AS     'order_age'
	,#ORDER_INFO.AuthDeptName AS                                'ordering_department_name'
	,#ORDER_INFO.OrderProvSpecialty AS                           'ordering_provider_specialty'
	-- Procedure / Diagnosis
	,#ORDER_INFO.ProcID AS                                      'procedure_id'
	,#ORDER_INFO.ProcCode AS                                    'procedure_code'
	,#VISIT_DIAGNOSIS.DxCode AS                                 'dx_code'
	,#VISIT_DIAGNOSIS.DxName AS                                 'dx_description'
	,#ORDER_CPT.CPTCode AS                                      'cpt_code'
	,#ORDER_CPT.Description AS                                  'cpt_procedure_description'
	,#ORDER_CPT.NameHistory AS                                  'cpt_description'
	,#ORDER_CPT.ContactDate AS                                  'max_procedure_contact_date'
	-- Appointment / Scheduling
	,#SCHEDULE_INFO.AppointmentID AS                            'appointment_id'
	,#SCHEDULE_INFO.ContactDate AS                              'appointment_date'
	,#SCHEDULE_INFO.ApptStatus AS                               'appointment_status'
	,#SCHEDULE_INFO.ApptSchedStatus AS                          'appointment_scheduling_status'
	,#ORDER_INFO.ApptReqStatus AS                               'appointment_request_status'
	,#REFERRAL_INFO.SchedStatus AS                              'scheduling_status'
	,#SCHEDULE_INFO.NoShowFlag AS                               'no_show_flag'
	-- Insurance
	,#ORDER_INFO.FinancialClassName AS                          'financial_class'
	,#INSURANCE_INFO.PatBenefitPlanName AS                      'insurance_1_code'
	,#INSURANCE_INFO.PatPayorName AS                            'insurance_1_payor_name'
	,#INSURANCE_INFO.SubscrName AS                              'insurance_1_policy_holder'
	,#INSURANCE_INFO.SubscrName AS                              'insurance_1_member_name'
	,#INSURANCE_INFO.SubscrNum AS                               'insurance_1_member_id'
	,#INSURANCE_INFO.GroupNum AS                                 'insurance_1_group_number'
	,#INSURANCE_INFO.PatMemEffFromDate AS                       'insurance_1_effective_date'
	,#INSURANCE_INFO.PatMemEffToDate AS                         'insurance_1_expiration_date'
	,#INSURANCE_INFO.BpAddrLine1 AS                             'insurance_1_address1'
	,#INSURANCE_INFO.BpAddrLine2 AS                             'insurance_1_address2'
	,#INSURANCE_INFO.BpCity AS                                  'insurance_1_city'
	,#INSURANCE_INFO.BpState AS                                 'insurance_1_state'
	,#INSURANCE_INFO.BpZip AS                                   'insurance_1_zip'
	,#INSURANCE_INFO.BpPhone AS                                 'insurance_1_phone'
	,#INSURANCE_INFO.BpFax AS                                   'insurance_1_fax'
	-- Facilities
	,#ORDER_INFO.Region AS                                      'region'
	,#ORDER_INFO.Campus AS                                      'campus'
	,#ORDER_INFO.LocName AS                                     'location'
	,#ORDER_INFO.PosLocName AS                                  'referred_from_location'
	,#ORDER_INFO.LocAdd AS                                      'referred_from_location_address1'
	,#ORDER_INFO.LocAdd2 AS                                     'referred_from_location_address2'
	,#ORDER_INFO.LocCity AS                                     'referred_from_location_city'
	,#ORDER_INFO.LocState AS                                    'referred_from_location_state'
	,#ORDER_INFO.LocZip AS                                      'referred_from_location_zip'
	,#ORDER_INFO.LocEmergPhone AS                               'referred_from_location_phone_number'
	,#ORDER_INFO.LocFaxNum AS                                   'referred_from_location_fax_number'
	-- Referred To
	,#REFERRAL_INFO.RefToProvNPI AS                             'referred_to_provider_npi'
	,#REFERRAL_INFO.RefToProv AS                                'referred_to_provider_name'
	,#REFERRAL_INFO.RsnForRfl AS                                'referred_to_reason'
	,#REFERRAL_INFO.RefToLocName AS                             'referred_to_location'
	,#REFERRAL_INFO.RefToAddr1 AS                               'referred_to_location_address'
	,#REFERRAL_INFO.RefToLocCity AS                              'referred_to_location_city'
	,#REFERRAL_INFO.RefToDept AS                                'referred_to_department'
	,#REFERRAL_INFO.RefToLocState AS                            'referred_to_location_state'
	,#REFERRAL_INFO.RefToLocZip AS                              'referred_to_location_zip'
	,#REFERRAL_INFO.RefToLocAreaCode AS                         'referred_to_location_area_code'
	,#REFERRAL_INFO.RefToLocPhone AS                            'referred_to_location_phone_number'
FROM
	#REFERRAL_INFO
	INNER JOIN #ORDER_INFO
		ON #REFERRAL_INFO.OrderID = #ORDER_INFO.OrderProcID
	LEFT JOIN #SCHEDULE_INFO
		ON #REFERRAL_INFO.ReferralID = #SCHEDULE_INFO.ReferralID
	LEFT JOIN #VISIT_DIAGNOSIS
		ON #ORDER_INFO.OrderCSN = #VISIT_DIAGNOSIS.VisitID
	LEFT JOIN #ORDER_CPT
		ON #ORDER_INFO.ProcID = #ORDER_CPT.ProcID
	LEFT JOIN #INSURANCE_INFO
		ON #REFERRAL_INFO.CvgID = #INSURANCE_INFO.PatCoverageID
		AND #INSURANCE_INFO.PatientID = #REFERRAL_INFO.PatientID
		AND #INSURANCE_INFO.RN = 1
		AND
		(
			(#INSURANCE_INFO.PatMemEffFromDate <= #ORDER_INFO.OrderingDate AND #INSURANCE_INFO.PatMemEffToDate >= #ORDER_INFO.OrderingDate)
			OR (#INSURANCE_INFO.PatMemEffFromDate <= #ORDER_INFO.OrderingDate AND #INSURANCE_INFO.PatMemEffToDate IS NULL)
		)
WHERE
	1=1
	AND #SCHEDULE_INFO.ContactDate IS NULL
ORDER BY 4, 1

----------------------------------------------------------------------------------------------------------
-- CLEANUP
----------------------------------------------------------------------------------------------------------

DROP TABLE #REFERRAL_INFO;
DROP TABLE #ORDER_INFO;
DROP TABLE #SCHEDULE_INFO;
DROP TABLE #VISIT_DIAGNOSIS;
DROP TABLE #ORDER_CPT;
DROP TABLE #INSURANCE_INFO;
