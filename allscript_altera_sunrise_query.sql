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
-- from an Allscripts/Altera Sunrise hosted environment.
-- It utilizes temporary tables and joins multiple data sources for comprehensive reporting.
--
-- Environment Notes:
-- - EHR Platform:       Allscripts Sunrise (Altera Digital Health)
-- - Hosting:            Hosted environment
-- - Primary Unique ID:  VisitID (patient visit identifier)
-- - Known Gap:          No-show filter is not currently being applied properly;
--                       the no-show appointment status is still captured as a field
--                       for downstream analysis and future filter correction.
-----------------------------------------------------------------------------------------------------------------
-- Table Mapping Reference (Epic Clarity → Allscripts Sunrise):
--
--   Epic Table               →  Allscripts Sunrise Table         Notes
--   -------------------------   --------------------------------  ------------------------------------------
--   PATIENT                  →  HPerson                          Core patient demographics
--   PAT_ENC                  →  HPatientVisit                    Encounters; VisitID is primary unique ID
--   ORDER_PROC               →  HOrder                           Orders
--   REFERRAL                 →  HReferral                        Referrals
--   REFERRAL_ORDER_ID        →  (HReferral.OrderID)              Direct FK in Allscripts
--   CLARITY_EAP              →  HCatalogMasterItem               Order catalog / procedure definitions
--   CLARITY_EAP_OT           →  HOrderProcedure + HServiceCode   CPT / procedure code mapping
--   CLARITY_SER              →  HStaff                           Providers / staff
--   CLARITY_SER_2            →  HStaff (NPI column)              Provider NPI
--   CLARITY_SER_SPEC         →  HStaffSpecialty + HDictionary     Provider specialty
--   CLARITY_DEP              →  HDepartment                      Departments
--   CLARITY_LOC / POS        →  HFacility                        Facilities / places of service
--   F_SCHED_APPT             →  HAppointment                     Appointment / scheduling
--   PAT_ENC_DX               →  HPatientVisitDiagnosis           Visit diagnoses
--   COVERAGE / CVG tables    →  HPatientInsurance                Patient insurance
--   V_COVERAGE_PAYOR_PLAN    →  HInsurancePlan + HInsuranceCarrier
--   CLARITY_EPM              →  HInsuranceCarrier                Payor / carrier info
--   CLARITY_EPP              →  HInsurancePlan                   Benefit plan info
--   CLARITY_FC               →  HDictionary (FinancialClass)     Financial class
--   APPT_REQUEST             →  HAppointment                     Appointment request status
--   ZC_* (all lookup tables) →  HDictionary                      Centralized dictionary/lookup table
--   PATIENT_RACE             →  HPersonRace                      Patient race
--   OTHER_COMMUNCTN          →  HPersonPhone                     Patient phone numbers
--
-- IMPORTANT: Table and column names may vary by Allscripts Sunrise version and
-- site-specific configuration. Please verify against your hosted reporting schema
-- (e.g., dbo.* or rpt.*) and adjust as needed. Columns referencing HDictionary
-- assume a centralized lookup table with DictionaryID/DisplayName; if your site
-- uses separate lookup tables, substitute accordingly.
-----------------------------------------------------------------------------------------------------------------
-- Filters:
-- @DATE FILTER
--      Retrieves referral records within the past 365 days.
-- @DECEASED FILTER
--      Excludes deceased patients (HPerson.DeceasedDate IS NOT NULL).
-- @ORDER TYPE FILTER
--      Removes order types not in scope (mapped from Epic order type codes).
-- @ORDER CLASS FILTER
--      Removes order classes not in scope.
-- @PROCEDURE CATEGORY FILTER
--      Removes procedure categories not in scope.
-- @ORDER EXPIRED FILTER
--      Excludes orders with expired expiration dates.
-- @SCHEDULING STATUS FILTER
--      Filters scheduling statuses. See note on no-show gap below.
-- @NO-SHOW GAP NOTE
--      The no-show filter is not currently being used properly in this environment.
--      The appointment status (including No Show) is captured as a reportable field
--      but is NOT used as an exclusion filter. This allows downstream analysis to
--      identify and address the gap. When the filter is corrected, add the appropriate
--      no-show exclusion to the #APPOINTMENT_INFO or final WHERE clause.
-----------------------------------------------------------------------------------------------------------------
-- Key Highlights:
-- - VisitID is used as the primary unique identifier per client requirements.
-- - Appointment Status and Scheduling Status are explicitly included as output fields.
-- - The HDictionary table replaces all Epic ZC_* lookup tables.
-- - Temporary tables are used for intermediate processing (verify temp table support
--   in your hosted environment; if restricted, convert to CTEs).
-----------------------------------------------------------------------------------------------------------------
*/


-----------------------------------------------------------------------------------------------------------------
-- TEMP TABLE 1: #REFERRAL_INFO
-- Captures referral records with patient demographics, referral status, and referred-to details.
-----------------------------------------------------------------------------------------------------------------

IF OBJECT_ID(N'TEMPDB..#REFERRAL_INFO') IS NOT NULL
	DROP TABLE #REFERRAL_INFO;

SELECT
	ref.ReferralID
	,ref.EntryDateTime
	,ref.CloseDateTime
	,ref.OrderID
	,ref.PersonID
	,p.MRN
	,payor.CarrierName                                        AS PRIMARY_PAYOR
	,ref_status_dict.DisplayName                              AS REFERRAL_STATUS
	,sched_status_dict.DisplayName                            AS SCHED_STATUS
	,p.FirstName                                              AS PAT_FIRST_NAME
	,p.LastName                                               AS PAT_LAST_NAME
	,p.MiddleName                                             AS PAT_MIDDLE_NAME
	,suffix_dict.DisplayName                                  AS SUFFIX
	,addr.AddressLine1                                        AS ADD_LINE_1
	,addr.AddressLine2                                        AS ADD_LINE_2
	,addr.City
	,addr.State                                               AS PATIENT_STATE
	,addr.PostalCode                                          AS ZIP
	,CAST(p.DateOfBirth AS DATE)                              AS PAT_DOB
	,p.SSN
	,cell_phone.PhoneNumber                                   AS PATIENT_CELL_PHONE
	,home_phone.PhoneNumber                                   AS HOME_PHONE
	,work_phone.PhoneNumber                                   AS WORK_PHONE
	,email.EmailAddress                                       AS EMAIL_ADDRESS
	,sex_dict.DisplayName                                     AS PATIENT_SEX
	,ethnicity_dict.DisplayName                               AS PATIENT_ETHNICITY
	,marital_dict.DisplayName                                 AS PATIENT_MARITAL_STATUS
	,race_dict.DisplayName                                    AS PATIENT_RACE
	,lang_dict.DisplayName                                    AS PATIENT_LANGUAGE
	,ref_to_prov.LastName + ', ' + ref_to_prov.FirstName      AS REF_TO_PROV
	,ref_to_prov.NPI                                          AS REF_TO_PROV_NPI
	,reason_dict.DisplayName                                  AS RSN_FOR_RFL
	,pi.PatientInsuranceID                                    AS INSURANCE_ID
	,p.DeceasedDate                                           AS PAT_DECEASED_DATE
	,p.ConsentToSMS                                           AS SEND_SMS_YN
	,p.ConsentToEmail                                         AS NOTIF_PAT_EMAIL_YN
	,ref_class_dict.DisplayName                               AS RFL_CLASS
	,ref_type_dict.DisplayName                                AS RFL_TYPE
	,ref_detail_dict.DisplayName                              AS REF_STATUS_DETAIL
	,pat_dept_spec_dict.DisplayName                           AS PAT_SPECIALTY
	,ref_to_fac.FacilityName                                  AS REF_TO_LOC_NAME
	,ref_to_fac.AddressLine1                                  AS REF_TO_ADDR_1
	,ref_to_fac.City                                          AS REF_TO_LOC_CITY
	,ref_to_dept.DepartmentName                               AS REF_TO_DEPT
	,ref_to_fac.State                                         AS REF_TO_LOC_STATE
	,ref_to_fac.PostalCode                                    AS REF_TO_LOC_ZIP
	,ref_to_fac.AreaCode                                      AS REF_TO_LOC_AREA_CODE
	,ref_to_fac.PhoneNumber                                   AS REF_TO_LOC_PHONE

INTO #REFERRAL_INFO

FROM
	HReferral ref
	INNER JOIN HOrder ref_ord ON ref.OrderID = ref_ord.OrderID
	LEFT JOIN HPerson p ON ref.PersonID = p.PersonID
	LEFT JOIN HDictionary sex_dict ON p.SexDictionaryID = sex_dict.DictionaryID
	LEFT JOIN HPersonAddress addr
		ON p.PersonID = addr.PersonID
		AND addr.IsActive = 1
		AND addr.IsPrimary = 1
	LEFT JOIN HPersonPhone home_phone
		ON p.PersonID = home_phone.PersonID
		AND home_phone.PhoneTypeCode = 'HOME'
		AND home_phone.IsPreferred = 1
	LEFT JOIN HPersonPhone work_phone
		ON p.PersonID = work_phone.PersonID
		AND work_phone.PhoneTypeCode = 'WORK'
	LEFT JOIN HPersonPhone cell_phone
		ON p.PersonID = cell_phone.PersonID
		AND cell_phone.PhoneTypeCode = 'CELL'
	LEFT JOIN HPersonEmail email
		ON p.PersonID = email.PersonID
		AND email.IsPreferred = 1
	LEFT JOIN HPatientInsurance pi
		ON ref.PersonID = pi.PersonID
		AND pi.FilingOrder = 1
		AND (pi.ExpirationDate IS NULL OR pi.ExpirationDate >= CAST(GETDATE() AS DATE))
	LEFT JOIN HInsurancePlan ip ON pi.InsurancePlanID = ip.InsurancePlanID
	LEFT JOIN HInsuranceCarrier payor ON ip.CarrierID = payor.CarrierID
	LEFT JOIN HDictionary ref_status_dict ON ref.ReferralStatusDictionaryID = ref_status_dict.DictionaryID
	LEFT JOIN HDictionary sched_status_dict ON ref.SchedulingStatusDictionaryID = sched_status_dict.DictionaryID
	LEFT JOIN HDictionary suffix_dict ON p.SuffixDictionaryID = suffix_dict.DictionaryID
	LEFT JOIN HDictionary ethnicity_dict ON p.EthnicityDictionaryID = ethnicity_dict.DictionaryID
	LEFT JOIN HDictionary marital_dict ON p.MaritalStatusDictionaryID = marital_dict.DictionaryID
	LEFT JOIN HPersonRace pr ON p.PersonID = pr.PersonID AND pr.Line = 1
	LEFT JOIN HDictionary race_dict ON pr.RaceDictionaryID = race_dict.DictionaryID
	LEFT JOIN HDictionary lang_dict ON p.LanguageDictionaryID = lang_dict.DictionaryID
	LEFT JOIN HStaff ref_to_prov ON ref.ReferredToProviderID = ref_to_prov.StaffID
	LEFT JOIN HDictionary reason_dict ON ref.ReasonDictionaryID = reason_dict.DictionaryID
	LEFT JOIN HDictionary ref_class_dict ON ref.ReferralClassDictionaryID = ref_class_dict.DictionaryID
	LEFT JOIN HDictionary ref_type_dict ON ref.ReferralTypeDictionaryID = ref_type_dict.DictionaryID
	LEFT JOIN HDictionary ref_detail_dict ON ref.RefStatusDetailDictionaryID = ref_detail_dict.DictionaryID
	LEFT JOIN HDepartment ref_to_dept ON ref.ReferredToDepartmentID = ref_to_dept.DepartmentID
	LEFT JOIN HDepartment pat_dept ON p.PrimaryDepartmentID = pat_dept.DepartmentID
	LEFT JOIN HDictionary pat_dept_spec_dict ON pat_dept.SpecialtyDictionaryID = pat_dept_spec_dict.DictionaryID
	LEFT JOIN HFacility ref_to_fac ON ref.ReferredToFacilityID = ref_to_fac.FacilityID
WHERE
	1=1
	------------------------------------
	--@DATE FILTER
	AND CAST(ref.EntryDateTime AS DATE) >= DATEADD(DAY, -365, CAST(GETDATE() AS DATE))
	------------------------------------
	--@DECEASED FILTER
	AND p.DeceasedDate IS NULL
	------------------------------------
	--@SCHEDULING STATUS FILTER
	-- Adjust these values to match your Allscripts Sunrise dictionary entries.
	-- The no-show status is intentionally NOT excluded here (see Known Gap note above).
	AND ISNULL(sched_status_dict.DisplayName, '') NOT IN
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
-----------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------
-- TEMP TABLE 2: #ORDER_INFO
-- Captures order details including ordering provider, department, facility, and status.
-- VisitID is the primary unique identifier per client requirements.
-----------------------------------------------------------------------------------------------------------------

IF OBJECT_ID(N'TEMPDB..#ORDER_INFO') IS NOT NULL
	DROP TABLE #ORDER_INFO;

SELECT
	ord.OrderID
	,ord.VisitID
	,CAST(ord.OrderDateTime AS DATE)                          AS ORDERING_DATE
	,ord.EntryDateTime                                        AS ORDER_INST
	,ord.Description                                          AS DESCRIPTION
	,ord.VisitID                                              AS ORDER_VISIT_ID
	,ord.PersonID
	,order_status_dict.DisplayName                            AS ORDER_STATUS
	,order_result_dict.DisplayName                            AS ORDER_RESULT_STATUS
	,cmi.ProcedureCode                                        AS PROC_CODE
	,cmi.DisplayName                                          AS PROC_NAME
	,cmi.OrderDisplayName                                     AS ORDER_DISPLAY_NAME
	,ord_prov.LastName + ', ' + ord_prov.FirstName            AS AUTH_PROVIDER
	,ord_prov.NPI                                             AS ORDER_PROV_NPI
	,ord_prov_spec_dict.DisplayName                           AS ORDER_PROV_SPECIALTY
	,order_type_dict.DisplayName                              AS ORDER_TYPE
	,ord_dept.DepartmentName                                  AS DEPARTMENT_NAME
	,auth_dept.DepartmentName                                 AS AUTH_DEPT_NAME
	,ord_fac.FacilityName                                     AS LOC_NAME
	,region_dict.DisplayName                                  AS REGION
	,campus_dict.DisplayName                                  AS CAMPUS
	,ord.CatalogMasterItemID                                  AS PROC_ID
	,ord.ExpirationDate                                       AS STANDING_EXP_DATE
	,pv.AccountNumber                                         AS HSP_ACCOUNT_ID
	,CAST(pv.VisitDateTime AS DATE)                           AS ENCOUNTER_CONTACT_DATE
	,pat_class_dict.DisplayName                               AS PAT_CLASS
	,enc_dept.DepartmentName                                  AS PAT_ENC_DEP
	,enc_fac.FacilityName                                     AS POS_LOC_NAME
	,enc_fac.AddressLine1                                     AS LOC_ADD
	,enc_fac.AddressLine2                                     AS LOC_ADD_2
	,enc_fac.City                                             AS LOC_CITY
	,enc_fac.State                                            AS LOC_STATE
	,enc_fac.PostalCode                                       AS LOC_ZIP
	,enc_fac.PhoneNumber                                      AS LOC_PHONE
	,enc_fac.FaxNumber                                        AS LOC_FAX_NUM
	,fin_class_dict.DisplayName                               AS FINANCIAL_CLASS_NAME
	,appt_status_dict.DisplayName                             AS APPT_STATUS
	,appt_sched_dict.DisplayName                              AS APPT_SCHED_STATUS

INTO #ORDER_INFO

FROM
	HOrder ord
	LEFT JOIN HDictionary order_status_dict ON ord.OrderStatusDictionaryID = order_status_dict.DictionaryID
	LEFT JOIN HDictionary order_result_dict ON ord.OrderResultStatusDictionaryID = order_result_dict.DictionaryID
	LEFT JOIN HCatalogMasterItem cmi ON ord.CatalogMasterItemID = cmi.CatalogMasterItemID
	LEFT JOIN HStaff ord_prov ON ord.AuthorizingProviderID = ord_prov.StaffID
	LEFT JOIN HStaffSpecialty ord_prov_spec
		ON ord_prov.StaffID = ord_prov_spec.StaffID
		AND ord_prov_spec.Line = 1
	LEFT JOIN HDictionary ord_prov_spec_dict ON ord_prov_spec.SpecialtyDictionaryID = ord_prov_spec_dict.DictionaryID
	LEFT JOIN HDictionary order_type_dict ON ord.OrderTypeDictionaryID = order_type_dict.DictionaryID
	LEFT JOIN HDepartment ord_dept ON ord.DepartmentID = ord_dept.DepartmentID
	LEFT JOIN HDepartment auth_dept ON ord.AuthorizingDepartmentID = auth_dept.DepartmentID
	LEFT JOIN HFacility ord_fac ON ord_dept.FacilityID = ord_fac.FacilityID
	LEFT JOIN HDictionary region_dict ON ord_fac.RegionDictionaryID = region_dict.DictionaryID
	LEFT JOIN HDictionary campus_dict ON ord_fac.CampusDictionaryID = campus_dict.DictionaryID
	LEFT JOIN HPatientVisit pv ON ord.VisitID = pv.VisitID
	LEFT JOIN HDictionary pat_class_dict ON pv.PatientClassDictionaryID = pat_class_dict.DictionaryID
	LEFT JOIN HDepartment enc_dept ON pv.DepartmentID = enc_dept.DepartmentID
	LEFT JOIN HFacility enc_fac ON enc_dept.FacilityID = enc_fac.FacilityID
	LEFT JOIN HDictionary fin_class_dict ON pv.FinancialClassDictionaryID = fin_class_dict.DictionaryID
	LEFT JOIN HAppointment appt
		ON ord.VisitID = appt.VisitID
		AND ord.PersonID = appt.PersonID
	LEFT JOIN HDictionary appt_status_dict ON appt.AppointmentStatusDictionaryID = appt_status_dict.DictionaryID
	LEFT JOIN HDictionary appt_sched_dict ON appt.SchedulingStatusDictionaryID = appt_sched_dict.DictionaryID
WHERE
	1=1
	---------------------------------------------------------------------------------------------------------------
	--@ORDER TYPE FILTER
	-- Allscripts Sunrise uses dictionary-based order types. Map the excluded Epic numeric codes
	-- to the corresponding DisplayName values in your HDictionary table. Common exclusions:
	-- Micro Biology, Immunizations, Lab, Nursing, OT, PT, Respiratory Care, Point of Care,
	-- PFT, IV, Dialysis, Pathology, Device Check, Medications, OB Ultra Sound, Surgery Request, Lab External
	AND ISNULL(order_type_dict.DisplayName, '') NOT IN
	(
		'Micro Biology'
		,'Immunizations'
		,'Lab'
		,'Nursing'
		,'Occupational Therapy'
		,'Physical Therapy'
		,'Respiratory Care'
		,'Point of Care'
		,'Pulmonary Function Test'
		,'IV'
		,'Dialysis'
		,'Pathology'
		,'Device Check'
		,'Medications'
		,'OB Ultra Sound'
		,'Surgery Request'
		,'Lab External'
	)
	---------------------------------------------------------------------------------------------------------------
	--@ORDER CLASS FILTER
	-- Map Epic order class codes to Allscripts dictionary values.
	-- Excluded: Normal, Point of Care, Historical, Lab Collect, OTC, Phone In,
	-- No Print, Unit Collect, Print, Clinic Performed, Clinic Collect
	AND ISNULL(ord.OrderClassCode, '') NOT IN
	(
		'NORMAL'
		,'POC'
		,'HISTORICAL'
		,'LAB_COLLECT'
		,'OTC'
		,'PHONE_IN'
		,'NO_PRINT'
		,'UNIT_COLLECT'
		,'PRINT'
		,'CLINIC_PERFORMED'
		,'CLINIC_COLLECT'
	)
	---------------------------------------------------------------------------------------------------------------
	--@PROCEDURE CATEGORY FILTER
	-- Map Epic procedure category IDs to Allscripts catalog category names.
	-- Excluded categories: Nursing Assessment, Nursing Informational, ADT, Nursing Treatment,
	-- Diet, General Supply, Nursing Education, Precaution, Blood Admin, Pharmacy Consult,
	-- Nursing Med Communication, Neonate Therapies
	AND ISNULL(cmi.CategoryCode, '') NOT IN
	(
		'NURSING_ASSESSMENT'
		,'NURSING_INFORMATIONAL'
		,'ADT'
		,'NURSING_TREATMENT'
		,'DIET'
		,'GENERAL_SUPPLY'
		,'NURSING_EDUCATION'
		,'PRECAUTION'
		,'BLOOD_ADMIN'
		,'PHARMACY_CONSULT'
		,'NURSING_MED_COMM'
		,'NEONATE_THERAPIES'
	)
	---------------------------------------------------------------------------------------------------------------
	--@ORDER EXPIRED FILTER
	AND (ord.ExpirationDate IS NULL OR ord.ExpirationDate >= CAST(GETDATE() AS DATE))
-----------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------
-- TEMP TABLE 3: #APPOINTMENT_INFO
-- Captures appointment/scheduling data for referrals in #REFERRAL_INFO.
-- Includes appointment status and scheduling status per client requirements.
-- NOTE: No-show records are intentionally NOT filtered out (known gap).
-----------------------------------------------------------------------------------------------------------------

IF OBJECT_ID(N'TEMPDB..#APPOINTMENT_INFO') IS NOT NULL
	DROP TABLE #APPOINTMENT_INFO;

SELECT
	appt.AppointmentID
	,appt.VisitID
	,appt.PersonID
	,appt.ReferralID
	,CAST(appt.AppointmentDateTime AS DATE)                   AS CONTACT_DATE
	,appt_status_dict.DisplayName                             AS APPT_STATUS
	,appt_sched_dict.DisplayName                              AS APPT_SCHED_STATUS
	-- No-show flag captured for reference; filter is not applied (known gap)
	,CASE
		WHEN appt_status_dict.DisplayName = 'No Show' THEN 'Y'
		ELSE 'N'
	 END                                                      AS NO_SHOW_FLAG

INTO #APPOINTMENT_INFO

FROM
	HAppointment appt
	LEFT JOIN HDictionary appt_status_dict ON appt.AppointmentStatusDictionaryID = appt_status_dict.DictionaryID
	LEFT JOIN HDictionary appt_sched_dict ON appt.SchedulingStatusDictionaryID = appt_sched_dict.DictionaryID
WHERE
	appt.ReferralID IN
	(
		SELECT DISTINCT
			ReferralID
		FROM
			#REFERRAL_INFO
	)
-----------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------
-- TEMP TABLE 4: #VISIT_DX
-- Captures primary diagnosis for each visit.
-- Equivalent to Epic's #PAT_ENC_DX using VisitID instead of PAT_ENC_CSN_ID.
-----------------------------------------------------------------------------------------------------------------

IF OBJECT_ID(N'TEMPDB..#VISIT_DX') IS NOT NULL
	DROP TABLE #VISIT_DX;

SELECT
	vdx.VisitID
	,COALESCE(vdx.ICD10Code, vdx.ICD9Code)                   AS DX_CODE
	,vdx.DiagnosisDescription                                 AS DX_NAME

INTO #VISIT_DX

FROM
	HPatientVisitDiagnosis vdx
	INNER JOIN HPatientVisit pv ON vdx.VisitID = pv.VisitID
WHERE
	pv.VisitID IN
	(
		SELECT DISTINCT
			ORDER_VISIT_ID
		FROM
			#ORDER_INFO
	)
	AND vdx.IsPrimary = 1
-----------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------
-- TEMP TABLE 5: #ORDER_CPT
-- Captures the most recent CPT code for each catalog master item (procedure).
-- Equivalent to Epic's #ORDER_PROC_CPT using HOrderProcedure + HServiceCode.
-----------------------------------------------------------------------------------------------------------------

IF OBJECT_ID(N'TEMPDB..#ORDER_CPT') IS NOT NULL
	DROP TABLE #ORDER_CPT;

SELECT *

INTO #ORDER_CPT

FROM
	(
	SELECT
		op.CatalogMasterItemID                                AS PROC_ID
		,op.EffectiveDate                                     AS CONTACT_DATE
		,sc.CPTCode                                           AS CPT_CODE
		,sc.Description                                       AS DESCRIPTION
		,sc.NameHistory                                       AS NAME_HISTORY
		,ROW_NUMBER() OVER (
			PARTITION BY op.CatalogMasterItemID
			ORDER BY op.EffectiveDate DESC
		)                                                     AS RN
	FROM
		HOrderProcedure op
		LEFT JOIN HServiceCode sc ON op.ServiceCodeID = sc.ServiceCodeID
	WHERE
		op.CatalogMasterItemID IN
		(
			SELECT DISTINCT
				PROC_ID
			FROM
				#ORDER_INFO
		)
	) A
WHERE
	A.RN = 1
-----------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------
-- TEMP TABLE 6: #INSURANCE_INFO
-- Captures insurance/coverage details for patients in #REFERRAL_INFO.
-- Equivalent to Epic's #CVG_INFO using HPatientInsurance, HInsurancePlan, HInsuranceCarrier.
-----------------------------------------------------------------------------------------------------------------

IF OBJECT_ID(N'TEMPDB..#INSURANCE_INFO') IS NOT NULL
	DROP TABLE #INSURANCE_INFO;

SELECT *

INTO #INSURANCE_INFO

FROM
	(
	SELECT
		ip.PlanName                                               AS PAT_BENEFIT_PLAN_NAME
		,ic.CarrierName                                           AS PAT_PAYOR_NAME
		,pi.SubscriberName                                        AS SUBSCR_NAME
		,pi.MemberID                                              AS SUBSCR_NUM
		,pi.GroupNumber                                            AS GROUP_NUM
		,pi.EffectiveDate                                          AS PAT_MEM_EFF_FROM_DATE
		,pi.ExpirationDate                                         AS PAT_MEM_EFF_TO_DATE
		,ic.AddressLine1                                           AS BP_ADDR_LINE1
		,ic.AddressLine2                                           AS BP_ADDR_LINE2
		,ic.City                                                   AS BP_CITY
		,ic.State                                                  AS BP_STATE
		,ic.PostalCode                                             AS BP_ZIP
		,ic.PhoneNumber                                            AS BP_PHONE
		,ic.FaxNumber                                              AS BP_FAX
		,pi.PatientInsuranceID                                     AS PAT_INSURANCE_ID
		,pi.PersonID
		,pi.PatientInsuranceID                                     AS COVERAGE_ID
		,ic.CarrierID                                              AS PAYOR_ID
		,ROW_NUMBER() OVER (
			PARTITION BY pi.PersonID, pi.PatientInsuranceID
			ORDER BY pi.ExpirationDate DESC, pi.FilingOrder ASC
		)                                                          AS RN
	FROM
		HPatientInsurance pi
		LEFT JOIN HInsurancePlan ip ON pi.InsurancePlanID = ip.InsurancePlanID
		LEFT JOIN HInsuranceCarrier ic ON ip.CarrierID = ic.CarrierID
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
				INSURANCE_ID
			FROM
				#REFERRAL_INFO
		)
	) INS
-----------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------
-- FINAL SELECT
-- Joins all temp tables and returns the comprehensive dataset for Abax Health.
-- Output field names match the Epic production query for consistency.
-- VisitID serves as the primary unique identifier.
-----------------------------------------------------------------------------------------------------------------

SELECT DISTINCT
	-- Patient
	#REFERRAL_INFO.PersonID                                                  AS 'patient_id'
	,#ORDER_INFO.VisitID                                                     AS 'visit_id'
	,#ORDER_INFO.HSP_ACCOUNT_ID                                              AS 'account_number'
	,#REFERRAL_INFO.MRN                                                      AS 'patient_mrn'
	,#REFERRAL_INFO.PAT_FIRST_NAME                                           AS 'patient_first_name'
	,#REFERRAL_INFO.PAT_LAST_NAME                                            AS 'patient_last_name'
	,#REFERRAL_INFO.PAT_MIDDLE_NAME                                          AS 'patient_middle_name'
	,#REFERRAL_INFO.SUFFIX                                                   AS 'patient_suffix'
	,#REFERRAL_INFO.ADD_LINE_1                                               AS 'patient_address1'
	,#REFERRAL_INFO.ADD_LINE_2                                               AS 'patient_address2'
	,#REFERRAL_INFO.City                                                     AS 'patient_city'
	,#REFERRAL_INFO.PATIENT_STATE                                            AS 'patient_state'
	,#REFERRAL_INFO.ZIP                                                      AS 'patient_zip'
	,#REFERRAL_INFO.PAT_DOB                                                  AS 'patient_birth_date'
	,#REFERRAL_INFO.SSN                                                      AS 'patient_ssn'
	,#REFERRAL_INFO.PATIENT_CELL_PHONE                                       AS 'patient_mobile_phone'
	,#REFERRAL_INFO.HOME_PHONE                                               AS 'patient_home_phone'
	,#REFERRAL_INFO.WORK_PHONE                                               AS 'patient_work_phone'
	,#REFERRAL_INFO.EMAIL_ADDRESS                                            AS 'patient_email'
	,#REFERRAL_INFO.PATIENT_SEX                                              AS 'patient_gender'
	,#REFERRAL_INFO.PATIENT_ETHNICITY                                        AS 'patient_ethnicity'
	,#REFERRAL_INFO.PATIENT_MARITAL_STATUS                                   AS 'patient_marital_status'
	,#REFERRAL_INFO.PATIENT_RACE                                             AS 'patient_primary_race'
	,#ORDER_INFO.PAT_CLASS                                                   AS 'patient_class'
	,#REFERRAL_INFO.PATIENT_LANGUAGE                                         AS 'patient_language'
	,CASE
		WHEN #REFERRAL_INFO.PAT_DECEASED_DATE IS NOT NULL THEN 'Y'
		ELSE 'N'
	 END                                                                     AS 'patient_deceased_flag'
	,#REFERRAL_INFO.SEND_SMS_YN                                              AS 'patient_opt_out_sms'
	,#REFERRAL_INFO.NOTIF_PAT_EMAIL_YN                                       AS 'patient_opt_out_email'
	,#REFERRAL_INFO.PAT_SPECIALTY                                            AS 'specialty'
	-- Encounter
	,#ORDER_INFO.PAT_ENC_DEP                                                 AS 'encounter_department'
	,#ORDER_INFO.ENCOUNTER_CONTACT_DATE                                      AS 'encounter_contact_date'
	-- Referral
	,#REFERRAL_INFO.ReferralID                                               AS 'referral_id'
	,#REFERRAL_INFO.RFL_CLASS                                                AS 'referral_class'
	,#REFERRAL_INFO.RFL_TYPE                                                 AS 'clinical_category'
	,#REFERRAL_INFO.REFERRAL_STATUS                                          AS 'referral_status'
	,#REFERRAL_INFO.REF_STATUS_DETAIL                                        AS 'referral_status_detail'
	,CAST(#REFERRAL_INFO.EntryDateTime AS DATE)                              AS 'referral_entry_date'
	,CAST(#REFERRAL_INFO.CloseDateTime AS DATE)                              AS 'referral_close_date'
	-- Order
	,#ORDER_INFO.OrderID                                                     AS 'order_id'
	,#ORDER_INFO.ORDER_TYPE                                                  AS 'order_type'
	,#ORDER_INFO.ORDERING_DATE                                               AS 'order_date'
	,CAST(#ORDER_INFO.ORDER_INST AS DATE)                                    AS 'order_entry_date'
	,#ORDER_INFO.STANDING_EXP_DATE                                           AS 'order_expiration_date'
	,#ORDER_INFO.PROC_NAME                                                   AS 'order_name'
	,#ORDER_INFO.AUTH_PROVIDER                                               AS 'order_provider_name'
	,#ORDER_INFO.ORDER_PROV_NPI                                              AS 'order_provider_npi'
	,#ORDER_INFO.ORDER_DISPLAY_NAME                                          AS 'order_mnemonic'
	,#ORDER_INFO.PROC_CODE                                                   AS 'order_procedure'
	,#ORDER_INFO.DESCRIPTION                                                 AS 'order_description'
	,#ORDER_INFO.ORDER_STATUS                                                AS 'order_status'
	,#ORDER_INFO.ORDER_RESULT_STATUS                                         AS 'order_result_status'
	,DATEDIFF(DAY, #ORDER_INFO.ORDERING_DATE, GETDATE())                     AS 'order_age'
	,#ORDER_INFO.AUTH_DEPT_NAME                                              AS 'ordering_department_name'
	,#ORDER_INFO.ORDER_PROV_SPECIALTY                                        AS 'ordering_provider_specialty'
	-- Procedure / Diagnosis
	,#ORDER_INFO.PROC_ID                                                     AS 'procedure_id'
	,#ORDER_INFO.PROC_CODE                                                   AS 'procedure_code'
	,#VISIT_DX.DX_CODE                                                       AS 'dx_code'
	,#VISIT_DX.DX_NAME                                                       AS 'dx_description'
	,#ORDER_CPT.CPT_CODE                                                     AS 'cpt_code'
	,#ORDER_CPT.DESCRIPTION                                                  AS 'cpt_procedure_description'
	,#ORDER_CPT.NAME_HISTORY                                                 AS 'cpt_description'
	,#ORDER_CPT.CONTACT_DATE                                                 AS 'max_procedure_contact_date'
	-- Appointment / Scheduling
	,#APPOINTMENT_INFO.AppointmentID                                         AS 'appointment_id'
	,#APPOINTMENT_INFO.CONTACT_DATE                                          AS 'appointment_date'
	,#APPOINTMENT_INFO.APPT_STATUS                                           AS 'appointment_status'
	-- In Allscripts Sunrise, appointment request status may not be a separate entity.
	-- This maps to the appointment status from the order-level join. Adjust if your
	-- Sunrise instance has a dedicated appointment request workflow table.
	,#ORDER_INFO.APPT_SCHED_STATUS                                           AS 'appointment_request_status'
	,#REFERRAL_INFO.SCHED_STATUS                                             AS 'scheduling_status'
	,#APPOINTMENT_INFO.APPT_SCHED_STATUS                                     AS 'appointment_scheduling_status'
	,#APPOINTMENT_INFO.NO_SHOW_FLAG                                          AS 'no_show_flag'
	-- Insurance
	,#ORDER_INFO.FINANCIAL_CLASS_NAME                                        AS 'financial_class'
	,#INSURANCE_INFO.PAT_BENEFIT_PLAN_NAME                                   AS 'insurance_1_code'
	,#INSURANCE_INFO.PAT_PAYOR_NAME                                          AS 'insurance_1_payor_name'
	,#INSURANCE_INFO.SUBSCR_NAME                                             AS 'insurance_1_policy_holder'
	,#INSURANCE_INFO.SUBSCR_NAME                                             AS 'insurance_1_member_name'
	,#INSURANCE_INFO.SUBSCR_NUM                                              AS 'insurance_1_member_id'
	,#INSURANCE_INFO.GROUP_NUM                                               AS 'insurance_1_group_number'
	,#INSURANCE_INFO.PAT_MEM_EFF_FROM_DATE                                   AS 'insurance_1_effective_date'
	,#INSURANCE_INFO.PAT_MEM_EFF_TO_DATE                                     AS 'insurance_1_expiration_date'
	,#INSURANCE_INFO.BP_ADDR_LINE1                                           AS 'insurance_1_address1'
	,#INSURANCE_INFO.BP_ADDR_LINE2                                           AS 'insurance_1_address2'
	,#INSURANCE_INFO.BP_CITY                                                 AS 'insurance_1_city'
	,#INSURANCE_INFO.BP_STATE                                                AS 'insurance_1_state'
	,#INSURANCE_INFO.BP_ZIP                                                  AS 'insurance_1_zip'
	,#INSURANCE_INFO.BP_PHONE                                                AS 'insurance_1_phone'
	,#INSURANCE_INFO.BP_FAX                                                  AS 'insurance_1_fax'
	-- Facilities
	,#ORDER_INFO.REGION                                                      AS 'region'
	,#ORDER_INFO.CAMPUS                                                      AS 'campus'
	,#ORDER_INFO.LOC_NAME                                                    AS 'location'
	,#ORDER_INFO.POS_LOC_NAME                                                AS 'referred_from_location'
	,#ORDER_INFO.LOC_ADD                                                     AS 'referred_from_location_address1'
	,#ORDER_INFO.LOC_ADD_2                                                   AS 'referred_from_location_address2'
	,#ORDER_INFO.LOC_CITY                                                    AS 'referred_from_location_city'
	,#ORDER_INFO.LOC_STATE                                                   AS 'referred_from_location_state'
	,#ORDER_INFO.LOC_ZIP                                                     AS 'referred_from_location_zip'
	-- Epic uses LOC_EMERG_PHONE; Allscripts maps to the facility's primary phone.
	-- Adjust to an emergency-specific phone column if available in your schema.
	,#ORDER_INFO.LOC_PHONE                                                   AS 'referred_from_location_phone_number'
	,#ORDER_INFO.LOC_FAX_NUM                                                 AS 'referred_from_location_fax_number'
	-- Referred To
	,#REFERRAL_INFO.REF_TO_PROV_NPI                                          AS 'referred_to_provider_npi'
	,#REFERRAL_INFO.REF_TO_PROV                                              AS 'referred_to_provider_name'
	,#REFERRAL_INFO.RSN_FOR_RFL                                              AS 'referred_to_reason'
	,#REFERRAL_INFO.REF_TO_LOC_NAME                                          AS 'referred_to_location'
	,#REFERRAL_INFO.REF_TO_ADDR_1                                            AS 'referred_to_location_address'
	,#REFERRAL_INFO.REF_TO_LOC_CITY                                          AS 'referred_to_location_city'
	,#REFERRAL_INFO.REF_TO_DEPT                                              AS 'referred_to_department'
	,#REFERRAL_INFO.REF_TO_LOC_STATE                                         AS 'referred_to_location_state'
	,#REFERRAL_INFO.REF_TO_LOC_ZIP                                           AS 'referred_to_location_zip'
	,#REFERRAL_INFO.REF_TO_LOC_AREA_CODE                                     AS 'referred_to_location_area_code'
	,#REFERRAL_INFO.REF_TO_LOC_PHONE                                         AS 'referred_to_location_phone_number'
FROM
	#REFERRAL_INFO
	INNER JOIN #ORDER_INFO ON #REFERRAL_INFO.OrderID = #ORDER_INFO.OrderID
	LEFT JOIN #APPOINTMENT_INFO ON #REFERRAL_INFO.ReferralID = #APPOINTMENT_INFO.ReferralID
	LEFT JOIN #VISIT_DX ON #ORDER_INFO.ORDER_VISIT_ID = #VISIT_DX.VisitID
	LEFT JOIN #ORDER_CPT ON #ORDER_INFO.PROC_ID = #ORDER_CPT.PROC_ID
	LEFT JOIN #INSURANCE_INFO ON #REFERRAL_INFO.INSURANCE_ID = #INSURANCE_INFO.COVERAGE_ID
		AND #INSURANCE_INFO.PersonID = #REFERRAL_INFO.PersonID
		AND #INSURANCE_INFO.RN = 1
		AND
		(
			(#INSURANCE_INFO.PAT_MEM_EFF_FROM_DATE <= #ORDER_INFO.ORDERING_DATE AND #INSURANCE_INFO.PAT_MEM_EFF_TO_DATE >= #ORDER_INFO.ORDERING_DATE)
			OR (#INSURANCE_INFO.PAT_MEM_EFF_FROM_DATE <= #ORDER_INFO.ORDERING_DATE AND #INSURANCE_INFO.PAT_MEM_EFF_TO_DATE IS NULL)
		)
WHERE
	1=1
	AND #APPOINTMENT_INFO.CONTACT_DATE IS NULL
ORDER BY 3, 1
-----------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------
-- CLEANUP
-- Drop all temporary tables.
-----------------------------------------------------------------------------------------------------------------

DROP TABLE #REFERRAL_INFO;
DROP TABLE #ORDER_INFO;
DROP TABLE #APPOINTMENT_INFO;
DROP TABLE #VISIT_DX;
DROP TABLE #ORDER_CPT;
DROP TABLE #INSURANCE_INFO;
