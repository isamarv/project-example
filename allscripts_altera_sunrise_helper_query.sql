/*
----------------------------------------------------------------------------------------------------
-- Abax Healthcare, Inc — ALLSCRIPTS / ALTERA (SUNRISE) HELPER QUERY
-- Purpose:
--   Sunrise-hosted extract aligned to the Epic helper output shape.
--
-- Notes:
--   1) Primary unique key anchor is VisitID (hosted Sunrise requirement).
--   2) Appointment status + scheduling status are explicitly included.
--   3) No-show records are intentionally NOT filtered out; a no-show flag is provided.
--   4) Replace SRC_* objects below with your hosted Sunrise views/tables.
--
-- Suggested source mapping (example only):
--   SRC_VISIT                  -> visit/encounter table with VisitID + contact date
--   SRC_PATIENT                -> patient demographics table
--   SRC_ORDER                  -> orders table
--   SRC_ORDER_CPT_HISTORY      -> order procedure/CPT history table
--   SRC_REFERRAL               -> referrals table
--   SRC_PROVIDER               -> provider table
--   SRC_DEPARTMENT             -> department table
--   SRC_LOCATION               -> location table
--   SRC_APPOINTMENT            -> appointment/scheduling table
--   SRC_APPOINTMENT_REQUEST    -> appointment request table
--   SRC_LOOKUP_*               -> code-to-name lookup tables
----------------------------------------------------------------------------------------------------
*/

WITH cpt_latest AS (
    SELECT
        cpt.OrderProcedureID,
        cpt.CptCode,
        cpt.CptDescription,
        cpt.ProcedureDescription,
        cpt.ContactDateTime,
        ROW_NUMBER() OVER (
            PARTITION BY cpt.OrderProcedureID
            ORDER BY cpt.ContactDateTime DESC
        ) AS rn
    FROM SRC_ORDER_CPT_HISTORY cpt
),
order_enriched AS (
    SELECT
        o.OrderProcedureID,
        o.OrderID,
        o.VisitID,
        o.PatientID,
        o.OrderDate,
        o.EntryDate,
        o.ProcedureCode,
        o.OrderName,
        o.OrderStatusCode,
        o.OrderResultStatusCode,
        o.ClinicalCategoryCode,
        o.ReferralClassCode,
        o.OrderingProviderID,
        o.OrderingDepartmentID,
        o.OrderingProviderSpecialty,
        cp.CptCode,
        cp.CptDescription,
        cp.ProcedureDescription,
        cp.ContactDateTime AS MaxProcedureContactDate
    FROM SRC_ORDER o
    LEFT JOIN cpt_latest cp
        ON cp.OrderProcedureID = o.OrderProcedureID
       AND cp.rn = 1
),
appt_enriched AS (
    SELECT
        a.VisitID,
        a.AppointmentID,
        a.AppointmentDateTime,
        a.AppointmentStatusCode,
        a.SchedulingStatusCode,
        a.NoShowReasonCode,
        CASE
            WHEN UPPER(COALESCE(sa.StatusName, '')) LIKE '%NO SHOW%' THEN 1
            WHEN a.NoShowReasonCode IS NOT NULL THEN 1
            ELSE 0
        END AS AppointmentNoShowFlag
    FROM SRC_APPOINTMENT a
    LEFT JOIN SRC_LOOKUP_APPOINTMENT_STATUS sa
        ON sa.StatusCode = a.AppointmentStatusCode
),
referral_enriched AS (
    SELECT
        r.ReferralID,
        r.OrderID,
        r.EntryDate AS ReferralEntryDate,
        r.CloseDate AS ReferralCloseDate,
        r.ReferralStatusCode,
        r.ReferralStatusDetailCode,
        r.ReferralClassCode,
        r.ReferralTypeCode,
        r.ReferredToLocationID,
        r.ReferredToDepartmentID
    FROM SRC_REFERRAL r
)

SELECT
    -- Hosted Sunrise primary unique anchor
    v.VisitID AS visit_id,

    -- Anonymized identifiers
    CONCAT(
        RIGHT(REPLICATE('0', 3) + CAST(ABS(CHECKSUM(NEWID())) % 1000 AS varchar(3)), 3),
        p.PatientMRN,
        RIGHT(REPLICATE('0', 3) + CAST(ABS(CHECKSUM(NEWID())) % 1000 AS varchar(3)), 3)
    ) AS anonymized_patient_mrn,
    YEAR(p.BirthDate) AS patient_birth_year,
    CONCAT(
        RIGHT(REPLICATE('0', 3) + CAST(ABS(CHECKSUM(NEWID())) % 1000 AS varchar(3)), 3),
        oe.OrderProcedureID,
        RIGHT(REPLICATE('0', 3) + CAST(ABS(CHECKSUM(NEWID())) % 1000 AS varchar(3)), 3)
    ) AS anonymized_order_id,

    -- Order/procedure
    CONVERT(varchar(10), oe.OrderDate, 110) AS order_date,
    CONVERT(varchar(10), oe.EntryDate, 110) AS entry_date,
    oe.OrderName AS order_name,
    oe.ProcedureCode AS order_procedure_code,
    oe.CptCode AS order_cpt_code,
    oe.ProcedureDescription AS order_procedure_code_description,
    lc.ClinicalCategoryName AS clinical_category,
    op.ProviderName AS ordering_physician_name,
    lrc.ReferralClassName AS referral_class,

    -- Referral
    re.ReferralID,
    loc.LocationName AS referred_to_location,
    loc.AddressLine1 AS referred_to_location_address,
    dept.DepartmentName AS referred_to_department,
    loc.PhoneNumber AS referred_to_location_phone,
    oe.CptCode AS cpt_code,
    p.PrimarySpecialty AS specialty,
    od.DepartmentName AS ordering_department_name,
    oe.OrderingProviderSpecialty AS ordering_provider_specialty,
    los.OrderStatusName AS order_status,
    lrs.ReferralStatusName AS referral_status,
    lors.OrderResultStatusName AS order_result_status,
    lrsd.ReferralStatusDetailName AS referral_status_detail,
    CONVERT(varchar(10), re.ReferralEntryDate, 110) AS referral_entry_date,
    CONVERT(varchar(10), re.ReferralCloseDate, 110) AS referral_close_date,
    lrc.ReferralClassName AS referral_class_name,
    lrt.ReferralTypeName AS referral_type_name,
    p.PatientID AS patient_id,
    v.ContactDateTime AS encounter_contact_date,
    v.EncounterCSN AS patient_encounter_csn,
    oe.MaxProcedureContactDate AS max_procedure_contact_date,
    lars.AppointmentRequestStatusName AS appointment_request_status,

    -- Requested additions for Sunrise delivery
    las.AppointmentStatusName AS appointment_status,
    lss.SchedulingStatusName AS scheduling_status,
    ae.AppointmentNoShowFlag AS appointment_no_show_flag,
    lnsr.NoShowReasonName AS appointment_no_show_reason

FROM SRC_VISIT v
INNER JOIN order_enriched oe
    ON oe.VisitID = v.VisitID
LEFT JOIN referral_enriched re
    ON re.OrderID = oe.OrderProcedureID
LEFT JOIN SRC_PATIENT p
    ON p.PatientID = oe.PatientID
LEFT JOIN SRC_PROVIDER op
    ON op.ProviderID = oe.OrderingProviderID
LEFT JOIN SRC_DEPARTMENT od
    ON od.DepartmentID = oe.OrderingDepartmentID
LEFT JOIN SRC_LOCATION loc
    ON loc.LocationID = re.ReferredToLocationID
LEFT JOIN SRC_DEPARTMENT dept
    ON dept.DepartmentID = re.ReferredToDepartmentID
LEFT JOIN appt_enriched ae
    ON ae.VisitID = v.VisitID
LEFT JOIN SRC_APPOINTMENT_REQUEST ar
    ON ar.OrderProcedureID = oe.OrderProcedureID

-- Lookups
LEFT JOIN SRC_LOOKUP_CLINICAL_CATEGORY lc
    ON lc.ClinicalCategoryCode = oe.ClinicalCategoryCode
LEFT JOIN SRC_LOOKUP_REFERRAL_CLASS lrc
    ON lrc.ReferralClassCode = re.ReferralClassCode
LEFT JOIN SRC_LOOKUP_REFERRAL_TYPE lrt
    ON lrt.ReferralTypeCode = re.ReferralTypeCode
LEFT JOIN SRC_LOOKUP_ORDER_STATUS los
    ON los.OrderStatusCode = oe.OrderStatusCode
LEFT JOIN SRC_LOOKUP_ORDER_RESULT_STATUS lors
    ON lors.OrderResultStatusCode = oe.OrderResultStatusCode
LEFT JOIN SRC_LOOKUP_REFERRAL_STATUS lrs
    ON lrs.ReferralStatusCode = re.ReferralStatusCode
LEFT JOIN SRC_LOOKUP_REFERRAL_STATUS_DETAIL lrsd
    ON lrsd.ReferralStatusDetailCode = re.ReferralStatusDetailCode
LEFT JOIN SRC_LOOKUP_APPOINTMENT_REQUEST_STATUS lars
    ON lars.AppointmentRequestStatusCode = ar.AppointmentRequestStatusCode
LEFT JOIN SRC_LOOKUP_APPOINTMENT_STATUS las
    ON las.StatusCode = ae.AppointmentStatusCode
LEFT JOIN SRC_LOOKUP_SCHEDULING_STATUS lss
    ON lss.SchedulingStatusCode = ae.SchedulingStatusCode
LEFT JOIN SRC_LOOKUP_NO_SHOW_REASON lnsr
    ON lnsr.NoShowReasonCode = ae.NoShowReasonCode

WHERE
    oe.OrderDate IS NOT NULL;
