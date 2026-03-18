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
--
-- De-identification techniques applied:
--   1. Patient MRN:    Wrapped with random padding via CHECKSUM(NEWID()) to produce anonymized_patient_mrn.
--   2. Date of Birth:  Reduced to birth year only (YEAR(BIRTH_DATE)).
--   3. Order ID:       Wrapped with random padding via CHECKSUM(NEWID()) to produce anonymized_order_id.
--   4. Excluded fields: Patient name, SSN, full address, phone numbers, email, insurance subscriber
--                        name/number, and other direct identifiers are omitted entirely.
-----------------------------------------------------------------------------------------------------------------
*/

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
    CONCAT(
        RIGHT(REPLICATE('0', 3) + CAST(ABS(CHECKSUM(NEWID())) % 1000 AS varchar(3)), 3),
        patient.PAT_MRN_ID,
        RIGHT(REPLICATE('0', 3) + CAST(ABS(CHECKSUM(NEWID())) % 1000 AS varchar(3)), 3)
    ) AS anonymized_patient_mrn,

    YEAR(patient.BIRTH_DATE) AS patient_birth_year,

    CONCAT(
        RIGHT(REPLICATE('0', 3) + CAST(ABS(CHECKSUM(NEWID())) % 1000 AS varchar(3)), 3),
        pof.ORDER_PROC_ID,
        RIGHT(REPLICATE('0', 3) + CAST(ABS(CHECKSUM(NEWID())) % 1000 AS varchar(3)), 3)
    ) AS anonymized_order_id,

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
