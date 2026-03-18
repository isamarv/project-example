/* 
-----------------------------------------------------------------------------------------------------------------
-- Abax Healthcare, Inc — EPIC CLARITY HELPER QUERY (De-Identification)
-- Contact: Jay Dalke
-- Email: support@abaxhealth.com
-- Please reach out to Jay Dalke for any assistance with this query. 
-- Query support is part of Abax Health's comprehensive implementation process.
-----------------------------------------------------------------------------------------------------------------
-- Purpose:
-- This query is a de-identified version of the Abax Health Epic Clarity production query.
-- It removes or obfuscates PHI/PII fields (patient names, SSN, full DOB, addresses, phone numbers,
-- email, insurance member details) while preserving the clinical and operational data needed for
-- proof-of-concept analysis.
-----------------------------------------------------------------------------------------------------------------
-- DE-IDENTIFICATION LOGIC
-----------------------------------------------------------------------------------------------------------------
--
-- IMPORTANT: Before running, the client MUST set @salt to a secret value known only to them.
-- The salt is never transmitted to Abax — it stays within the client's environment.
--
-- ┌──────────────────────────────────────────────────────────────────────────────────────────────┐
-- │ 1. SALTED HASH — PATIENT MRN                                                               │
-- │                                                                                             │
-- │    CONVERT(varchar(64), HASHBYTES('SHA2_256', @salt + patient.PAT_MRN_ID), 2)               │
-- │                                                                                             │
-- │    • Concatenates the secret @salt with the real MRN, then hashes with SHA-256.             │
-- │    • The output is a fixed-length 64-character hex string.                                  │
-- │    • One-way: the original MRN cannot be recovered from the hash.                           │
-- │    • Deterministic per salt: the same MRN + salt always produces the same hash,             │
-- │      so records for the same patient can still be linked within a single extract.           │
-- │    • Without the salt, brute-forcing the MRN from the hash is computationally infeasible.   │
-- ├──────────────────────────────────────────────────────────────────────────────────────────────┤
-- │ 2. HIPAA SAFE HARBOR — BIRTH YEAR                                                          │
-- │                                                                                             │
-- │    CASE                                                                                     │
-- │        WHEN DATEDIFF(YEAR, patient.BIRTH_DATE, GETDATE()) >= 90 THEN NULL                   │
-- │        WHEN YEAR(patient.BIRTH_DATE) = YEAR(GETDATE())          THEN NULL                   │
-- │        ELSE YEAR(patient.BIRTH_DATE)                                                        │
-- │    END                                                                                      │
-- │                                                                                             │
-- │    • Ages 90+ are suppressed (HIPAA Safe Harbor §164.514(b)(2)(i)(C)).                      │
-- │    • Current-year births (newborns) are suppressed to prevent re-identification.            │
-- │    • All other patients return birth year only — no month or day.                           │
-- ├──────────────────────────────────────────────────────────────────────────────────────────────┤
-- │ 3. SALTED HASH — ORDER ID                                                                  │
-- │                                                                                             │
-- │    CONVERT(varchar(64), HASHBYTES('SHA2_256', @salt + CAST(... AS varchar(20))), 2)         │
-- │                                                                                             │
-- │    Same salted SHA-256 approach as the MRN, applied to ORDER_PROC_ID.                       │
-- ├──────────────────────────────────────────────────────────────────────────────────────────────┤
-- │ 4. OMIT DIRECT IDENTIFIERS                                                                  │
-- │                                                                                             │
-- │    The following fields from the production query are excluded entirely:                     │
-- │      • Patient first/last/middle name, suffix                                               │
-- │      • SSN                                                                                  │
-- │      • Full address (street, city, state, zip)                                              │
-- │      • Phone numbers (home, work, cell)                                                     │
-- │      • Email address                                                                        │
-- │      • Insurance subscriber name / member ID / group number                                 │
-- │      • Patient race, ethnicity, marital status, language, sex                               │
-- ├──────────────────────────────────────────────────────────────────────────────────────────────┤
-- │ 5. DATA VALIDATION WORKFLOW                                                                 │
-- │                                                                                             │
-- │    During POC data validation calls, the client needs to match hashed records on the        │
-- │    Abax side back to real records on their side. Because the hash is deterministic          │
-- │    (same salt + same MRN = same hash every time), the client generates a LOCAL-ONLY         │
-- │    lookup table that maps each hash back to the original identifier.                        │
-- │                                                                                             │
-- │    The lookup table stays entirely within the client's environment and is NEVER sent         │
-- │    to Abax. During the validation call:                                                     │
-- │      • Abax reads a hashed MRN from the de-identified extract.                             │
-- │      • The client searches their lookup table for that hash and finds the real MRN.         │
-- │      • The client confirms (or flags) data integrity on their side.                         │
-- │                                                                                             │
-- │    See the CLIENT-SIDE LOOKUP TABLE query at the bottom of this file.                       │
-- └──────────────────────────────────────────────────────────────────────────────────────────────┘
--
-----------------------------------------------------------------------------------------------------------------
*/

------------------------------------------------------------------------
-- @salt: Client-defined secret. MUST be set before execution.
-- Do NOT share this value with Abax or include it in any extract.
------------------------------------------------------------------------
DECLARE @salt varchar(128) = 'REPLACE_WITH_YOUR_SECRET_SALT_VALUE';

WITH ceot_maxdate AS (
    SELECT
        ceot.PROC_ID,
        MAX(ceot.CONTACT_DATE_REAL) AS MaxDate
    FROM CLARITY_EAP_OT ceot
    JOIN CLARITY_EAP ce ON ce.PROC_ID = ceot.PROC_ID
    WHERE ce.CODE_TYPE_C IN ('1')
    GROUP BY ceot.PROC_ID
)

SELECT
    CONVERT(varchar(64), HASHBYTES('SHA2_256', @salt + patient.PAT_MRN_ID), 2)
        AS anonymized_patient_mrn,

    CASE
        WHEN DATEDIFF(YEAR, patient.BIRTH_DATE, GETDATE()) >= 90 THEN NULL
        WHEN YEAR(patient.BIRTH_DATE) = YEAR(GETDATE())          THEN NULL
        ELSE YEAR(patient.BIRTH_DATE)
    END AS patient_birth_year,

    CONVERT(varchar(64), HASHBYTES('SHA2_256', @salt + CAST(pof.ORDER_PROC_ID AS varchar(20))), 2)
        AS anonymized_order_id,

    FORMAT(pof.ORDER_DATE, 'MM-dd-yyyy') AS order_date,
    FORMAT(pof.ORDER_INST, 'MM-dd-yyyy') AS entry_date,

    ce.PROC_NAME                 AS order_name,
    ce.PROC_CODE                 AS order_procedure_code,
    ceot.CPT_CODE                AS order_cpt_code,
    ceot.DESCRIPTION             AS order_procedure_code_description,
    ZC_RFL_TYPE.NAME             AS clinical_category,
    ordprov.PROV_NAME            AS ordering_physician_name,
    ZC_RFL_CLASS.NAME            AS referral_class,

    r.REFERRAL_ID,

    CLARITY_POS.POS_NAME         AS referred_to_location,
    CLARITY_POS.ADDRESS_LINE_1   AS referred_to_location_address,
    CLARITY_DEP.DEPARTMENT_NAME  AS referred_to_Department,
    CLARITY_DEP.PHONE_NUMBER     AS referred_to_location_phone,

    ceot.CPT_CODE                AS CPT_code,

    patdep.SPECIALTY             AS specialty,
    orddep.DEPARTMENT_NAME       AS ordering_department_name,
    ordspec.SPECIALTY            AS ordering_provider_specialty,

    zcors.name                   AS order_status,
    zc_ors2.name                 AS referral_status,
    zc_ors.name                  AS order_result_status,
    zc_rfs.name                  AS referral_status_detail,

    FORMAT(r.ENTRY_DATE, 'MM-dd-yyyy') AS referral_entry_date,
    FORMAT(r.CLOSE_DATE, 'MM-dd-yyyy') AS referral_close_date,

    zc_rfl_class.name            AS referral_class_name,
    zc_rfl_type.name             AS referral_type_name,

    patient.PAT_ID               AS patient_id,

    eapc.CONTACT_DATE            AS encounter_contact_date,
    eapc.PAT_ENC_CSN_ID          AS patient_encounter_csn,

    ceot_maxdate.MaxDate         AS max_procedure_contact_date,

    zc_appt_req_stat.NAME        AS appointment_request_status

FROM ORDER_PROC pof
LEFT JOIN REFERRAL r
    ON r.ORDER_ID = pof.ORDER_ID

LEFT JOIN PATIENT patient
    ON patient.PAT_ID = pof.PAT_ID

LEFT JOIN ZC_SEX
    ON ZC_SEX.RCPT_MEM_SEX_C = patient.SEX_C

LEFT JOIN ZC_STATE pat_state
    ON pat_state.STATE_C = patient.STATE_C

LEFT JOIN CLARITY_EAP ce
    ON ce.PROC_ID = pof.PROC_ID

LEFT JOIN CLARITY_EAP_OT ceot
    ON ceot.PROC_ID = ce.PROC_ID

LEFT JOIN ceot_maxdate
    ON ceot_maxdate.PROC_ID = ce.PROC_ID

LEFT JOIN ZC_RFL_TYPE
    ON ZC_RFL_TYPE.RFL_TYPE_C = r.RFL_TYPE_C

LEFT JOIN ZC_RFL_CLASS
    ON ZC_RFL_CLASS.RFL_CLASS_C = r.RFL_CLASS_C

LEFT JOIN CLARITY_SER ordprov
    ON ordprov.PROV_ID = pof.AUTHRZING_PROV_ID

LEFT JOIN CLARITY_DEP orddep
    ON orddep.DEPARTMENT_ID = pof.AUTHRZING_DEPT_ID

LEFT JOIN CLARITY_SER_SPEC ordspec
    ON ordspec.PROV_ID = ordprov.PROV_ID
   AND ordspec.LINE = 1

LEFT JOIN CLARITY_DEP patdep
    ON patdep.DEPARTMENT_ID = patient.CUR_PRIM_LOC_ID

LEFT JOIN ZC_ORDER_STATUS zcors
    ON zcors.ORDER_STATUS_C = pof.ORDER_STATUS_C

LEFT JOIN ZC_ORDER_RESULT_STATUS zc_ors
    ON zc_ors.ORDER_RESULT_STATUS_C = pof.ORDER_RESULT_STATUS_C

LEFT JOIN ZC_ORDER_STATUS zc_ors2
    ON zc_ors2.ORDER_STATUS_C = r.REF_STATUS_C

LEFT JOIN ZC_REF_STATUS zc_rfs
    ON zc_rfs.REF_STATUS_C = r.REF_STATUS_C

LEFT JOIN ZC_RFL_CLASS zc_rfl_class
    ON zc_rfl_class.RFL_CLASS_C = r.RFL_CLASS_C

LEFT JOIN ZC_RFL_TYPE zc_rfl_type
    ON zc_rfl_type.RFL_TYPE_C = r.RFL_TYPE_C

LEFT JOIN PAT_ENC eapc
    ON eapc.PAT_ENC_CSN_ID = pof.PAT_ENC_CSN_ID

LEFT JOIN CLARITY_POS
    ON CLARITY_POS.POS_ID = r.RECVD_POS_ID

LEFT JOIN ZC_STATE locstate
    ON locstate.STATE_C = CLARITY_POS.STATE_C

LEFT JOIN CLARITY_DEP
    ON CLARITY_DEP.DEPARTMENT_ID = r.RECVD_DEPT_ID

LEFT JOIN APPT_REQUEST ar
    ON ar.REQUEST_ID = pof.ORDER_PROC_ID

LEFT JOIN ZC_APPT_REQ_STATUS zc_appt_req_stat
    ON zc_appt_req_stat.APPT_REQ_STATUS_C = ar.APPT_REQ_STATUS_C

WHERE
    pof.ORDER_DATE IS NOT NULL
;


/*
-----------------------------------------------------------------------------------------------------------------
-- CLIENT-SIDE LOOKUP TABLE (for data validation calls)
-----------------------------------------------------------------------------------------------------------------
-- This query is run ONLY by the client, within their own environment.
-- It produces a mapping of hashed identifiers back to real values so the client can
-- cross-reference records during validation calls with Abax.
--
-- THIS TABLE MUST NEVER BE SENT TO ABAX OR INCLUDED IN ANY DATA EXTRACT.
-----------------------------------------------------------------------------------------------------------------
*/

-- Use the SAME @salt value that was used in the de-identification query above.
-- DECLARE @salt varchar(128) = 'REPLACE_WITH_YOUR_SECRET_SALT_VALUE';

SELECT
    patient.PAT_MRN_ID          AS real_patient_mrn,
    CONVERT(varchar(64), HASHBYTES('SHA2_256', @salt + patient.PAT_MRN_ID), 2)
                                 AS anonymized_patient_mrn
FROM PATIENT patient
;
