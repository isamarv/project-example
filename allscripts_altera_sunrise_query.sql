/*
-----------------------------------------------------------------------------------------------------------------
-- Abax Healthcare, Inc
-- ALLSCRIPTS / ALTERA (SUNRISE) HELPER QUERY TEMPLATE
-- Purpose: Epic-to-Sunrise translation with equivalent output fields for referral/order workflows.
-- Environment: Hosted Sunrise
--
-- IMPORTANT:
-- 1) This template uses Sunrise-style logical table names. Map schema/table/column names to your hosted build.
-- 2) Primary unique identifier is VisitID.
-- 3) Known gap: no-show logic is surfaced (not filtered out) so downstream workflows can inspect it.
-- 4) Appointment + scheduling statuses are explicitly included in the output.
-----------------------------------------------------------------------------------------------------------------
*/

WITH proc_maxdate AS (
    SELECT
        opc.ProcedureID,
        MAX(opc.ContactDate) AS max_procedure_contact_date
    FROM sunrise.OrderProcedureCodeHistory opc
    GROUP BY
        opc.ProcedureID
),
appt_ranked AS (
    SELECT
        a.VisitID,
        a.AppointmentID,
        a.AppointmentDate,
        aps.StatusName AS appointment_status,
        ss.StatusName AS scheduling_status,
        ns.StatusName AS no_show_status,
        ROW_NUMBER() OVER (
            PARTITION BY a.VisitID
            ORDER BY a.AppointmentDate DESC, a.AppointmentID DESC
        ) AS rn
    FROM sunrise.Appointment a
    LEFT JOIN sunrise.AppointmentStatus aps
        ON aps.AppointmentStatusID = a.AppointmentStatusID
    LEFT JOIN sunrise.SchedulingStatus ss
        ON ss.SchedulingStatusID = a.SchedulingStatusID
    LEFT JOIN sunrise.NoShowStatus ns
        ON ns.NoShowStatusID = a.NoShowStatusID
)

SELECT
    CONCAT(
        RIGHT(REPLICATE('0', 3) + CAST(ABS(CHECKSUM(NEWID())) % 1000 AS varchar(3)), 3),
        CAST(p.MRN AS varchar(50)),
        RIGHT(REPLICATE('0', 3) + CAST(ABS(CHECKSUM(NEWID())) % 1000 AS varchar(3)), 3)
    ) AS anonymized_patient_mrn,
    YEAR(p.BirthDate) AS patient_birth_year,

    CONCAT(
        RIGHT(REPLICATE('0', 3) + CAST(ABS(CHECKSUM(NEWID())) % 1000 AS varchar(3)), 3),
        CAST(o.OrderID AS varchar(50)),
        RIGHT(REPLICATE('0', 3) + CAST(ABS(CHECKSUM(NEWID())) % 1000 AS varchar(3)), 3)
    ) AS anonymized_order_id,

    CONVERT(varchar(10), o.OrderDate, 110) AS order_date,
    CONVERT(varchar(10), o.EntryDate, 110) AS entry_date,

    o.OrderName AS order_name,
    o.ProcedureCode AS order_procedure_code,
    opc.CPTCode AS order_cpt_code,
    opc.CPTDescription AS order_procedure_code_description,
    rc.ClinicalCategoryName AS clinical_category,
    op.ProviderName AS ordering_physician_name,
    rcl.ReferralClassName AS referral_class,

    r.ReferralID AS REFERRAL_ID,
    v.VisitID AS visit_id,

    rtl.LocationName AS referred_to_location,
    rtl.AddressLine1 AS referred_to_location_address,
    rtd.DepartmentName AS referred_to_Department,
    rtl.PhoneNumber AS referred_to_location_phone,

    opc.CPTCode AS CPT_code,

    ps.SpecialtyName AS specialty,
    od.DepartmentName AS ordering_department_name,
    ops.ProviderSpecialtyName AS ordering_provider_specialty,

    os.OrderStatusName AS order_status,
    rs.ReferralStatusName AS referral_status,
    ors.OrderResultStatusName AS order_result_status,
    rsd.ReferralStatusDetailName AS referral_status_detail,

    CONVERT(varchar(10), r.EntryDate, 110) AS referral_entry_date,
    CONVERT(varchar(10), r.CloseDate, 110) AS referral_close_date,

    rcl.ReferralClassName AS referral_class_name,
    rt.ReferralTypeName AS referral_type_name,

    p.PatientID AS patient_id,
    v.EncounterContactDate AS encounter_contact_date,
    v.EncounterCSN AS patient_encounter_csn,

    pm.max_procedure_contact_date,

    ars.AppointmentRequestStatusName AS appointment_request_status,
    appt.appointment_status,
    appt.scheduling_status,
    appt.no_show_status,
    CASE
        WHEN UPPER(COALESCE(appt.no_show_status, '')) IN ('NO SHOW', 'NO-SHOW', 'NOSHOW') THEN 1
        ELSE 0
    END AS is_no_show

FROM sunrise.Visit v
LEFT JOIN sunrise.OrderFact o
    ON o.VisitID = v.VisitID
LEFT JOIN sunrise.Referral r
    ON r.OrderID = o.OrderID
LEFT JOIN sunrise.Patient p
    ON p.PatientID = v.PatientID
LEFT JOIN sunrise.OrderProcedureCode opc
    ON opc.ProcedureID = o.ProcedureID
LEFT JOIN proc_maxdate pm
    ON pm.ProcedureID = o.ProcedureID
LEFT JOIN sunrise.ReferralClinicalCategory rc
    ON rc.ClinicalCategoryID = r.ClinicalCategoryID
LEFT JOIN sunrise.Provider op
    ON op.ProviderID = o.OrderingProviderID
LEFT JOIN sunrise.ReferralClass rcl
    ON rcl.ReferralClassID = r.ReferralClassID
LEFT JOIN sunrise.ReferralType rt
    ON rt.ReferralTypeID = r.ReferralTypeID
LEFT JOIN sunrise.Location rtl
    ON rtl.LocationID = r.ReferredToLocationID
LEFT JOIN sunrise.Department rtd
    ON rtd.DepartmentID = r.ReferredToDepartmentID
LEFT JOIN sunrise.PatientSpecialty ps
    ON ps.SpecialtyID = p.PrimarySpecialtyID
LEFT JOIN sunrise.Department od
    ON od.DepartmentID = o.OrderingDepartmentID
LEFT JOIN sunrise.ProviderSpecialty ops
    ON ops.ProviderID = o.OrderingProviderID
   AND ops.IsPrimary = 1
LEFT JOIN sunrise.OrderStatus os
    ON os.OrderStatusID = o.OrderStatusID
LEFT JOIN sunrise.OrderResultStatus ors
    ON ors.OrderResultStatusID = o.OrderResultStatusID
LEFT JOIN sunrise.ReferralStatus rs
    ON rs.ReferralStatusID = r.ReferralStatusID
LEFT JOIN sunrise.ReferralStatusDetail rsd
    ON rsd.ReferralStatusDetailID = r.ReferralStatusDetailID
LEFT JOIN sunrise.AppointmentRequest ar
    ON ar.OrderID = o.OrderID
LEFT JOIN sunrise.AppointmentRequestStatus ars
    ON ars.AppointmentRequestStatusID = ar.AppointmentRequestStatusID
LEFT JOIN appt_ranked appt
    ON appt.VisitID = v.VisitID
   AND appt.rn = 1
WHERE
    o.OrderDate IS NOT NULL
    AND CAST(o.OrderDate AS date) >= DATEADD(DAY, -365, CAST(GETDATE() AS date))
    -- NOTE: No-show is intentionally not filtered out; see is_no_show/no_show_status output fields.
;
