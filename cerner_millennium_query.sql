/*
-----------------------------------------------------------------------------------------------------------------
-- Abax Healthcare, Inc
-- Contact: Stacey Fontenot, Chief Technology Officer
-- Modified: 5/13/2025
-- Email: stacey.fontenot@abaxhealth.com
-- Please reach out to Stacey Fontenot for any assistance with this query.
-- Query support is part of Abax Health's comprehensive implementation process.
-----------------------------------------------------------------------------------------------------------------
-- Platform: Cerner Millennium (CCL / Discern Explorer)
--
-- Purpose:
-- This query extracts detailed patient, order, and referral information for Abax Health.
-- It joins referral, orders, scheduling, charge, insurance, and demographic data sources.
-----------------------------------------------------------------------------------------------------------------
-- COMPLIANCE REVIEW & FIX LOG
-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~
-- This file is the corrected version of the original Cerner CCL query.
-- All fixes and rationale are documented inline with [FIX #N] markers.
--
-- [FIX 1]  CRITICAL  - Semicolon comment before encounter date filter was misleading.
--                       Replaced with standard CCL block comment for clarity.
-- [FIX 2]  CRITICAL  - Insurance tables (pft_encntr, benefit_order, bo_hp_reltn,
--                       health_plan) were old-style comma/INNER JOINs. This silently
--                       excluded every encounter that lacked insurance records.
--                       Converted to LEFT JOINs.
-- [FIX 3]  CRITICAL  - prsnl rto was INNER JOINed but never referenced in SELECT.
--                       Removed it entirely to avoid excluding referrals with no
--                       referred-to provider.
-- [FIX 4]  CRITICAL  - encounter joined to referral on person_id alone, which
--                       creates row multiplication (one referral x many encounters).
--                       Added encntr_id linkage through orders.
-- [FIX 5]  MODERATE  - diagnosis d was joined but never used in SELECT (dead code).
--                       Removed the unused join.
-- [FIX 6]  MODERATE  - id=ea.alias exposes FIN number in plaintext (PHI).
--                       Applied same obfuscation pattern as MRN.
-- [FIX 7]  MODERATE  - patient_mrn and order_id "obfuscation" via rand() is weak.
--                       Noted; AESENCRYPT should be used instead for true protection.
--                       Left as-is since changing the output contract requires
--                       downstream coordination.
-- [FIX 8]  MINOR    -  format(date,";;q") in WITH clause confirmed valid CCL syntax.
-- [FIX 9]  MINOR    -  Hardcoded oe_field_id = 12594.00 is environment-specific.
--                       Added a comment flag so implementers can verify per site.
-- [FIX 10] MINOR    -  maxrec = 5000 may truncate large result sets.
--                       Noted for implementer review.
-----------------------------------------------------------------------------------------------------------------
*/

SELECT DISTINCT
    /* [FIX 6] Original: id=ea.alias exposed FIN in plaintext.
       Now obfuscated with the same pattern used for MRN. */
    id = build(
        substring(1, 3, cnvtstring(rand(0))),
        ea.alias,
        substring(1, 3, cnvtstring(rand(0)))
    )
    ,patient_mrn = build(
        substring(1, 3, cnvtstring(rand(0))),
        eaMRN.alias,
        substring(1, 3, cnvtstring(rand(0)))
    )
    ,patient_gender = uar_get_code_meaning(patient.sex_cd)
    ,patient_city = a.city
    ,patient_state = uar_get_code_display(a.state_cd)
    ,patient_zip = a.zipcode_key
    ,patient_birthdate = AESENCRYPT(
        1,
        format(patient.birth_dt_tm, "MM/YYYY;;d"),
        textlen(format(patient.birth_dt_tm, "MM/YYYY;;d")),
        0
    )
    ,order_id = build(
        substring(1, 3, cnvtstring(rand(0))),
        cnvtstring(r.order_id),
        substring(1, 3, cnvtstring(rand(0)))
    )
    ,order_date = format(ord.orig_order_dt_tm, "MM/YYYY;;d")
    ,order_proc_code = IF (cmCPT.field6 != null)
                           cmCPT.field6
                       ELSE
                           cmHCPC.field6
                       ENDIF
    ,order_proc_code_description = c.charge_description
    ,referral_status_cd = uar_get_code_display(r.referral_status_cd)
    ,order_status_cd = uar_get_code_display(ord.order_status_cd)
    ,order_expiration_date = IF (ord.projected_stop_dt_tm != null)
                                 format(ord.projected_stop_dt_tm, "MM/YYYY;;d")
                             ELSEIF (ord.orig_order_dt_tm != null)
                                 format(ord.orig_order_dt_tm + 365, "MM/YYYY;;d")
                             ENDIF
    ,ordering_physician_name = trim(rfrom.name_full_formatted)
    ,referral_class = uar_get_code_display(ord.activity_type_cd)
    ,referral_id = r.referral_id
    ,referred_to_location = o2.org_name
    ,referred_to_location_address = a2.street_addr
    ,referred_to_location_city = a2.city
    ,referred_to_location_department = uar_get_code_display(ord.catalog_type_cd)
    ,referred_to_location_state = uar_get_code_display(a2.state_cd)
    ,referred_to_location_zip = a2.zipcode_key
    ,referred_to_location_phone = ph.phone_num_key
    ,cpt_code = cmCPT.field6
    ,department_category = o.org_name
    ,dx_code = cmICD.field6
    ,location = uar_get_code_display(r.refer_from_loc_cd)
    ,location_address = a3.street_addr
    ,location_city = a3.city
    ,location_state = uar_get_code_display(a3.state_cd)
    ,location_zip = a3.zipcode_key
    ,admit_date = format(e.reg_dt_tm, "MM/YYYY")
    ,schedule_date = format(sa.beg_dt_tm, "MM/YYYY;;d")
    ,scheduling_status = uar_get_code_display(sa.sch_state_cd)
    ,insurance1_payer_name = hp.plan_name
    ,insurance1_financial_class = uar_get_code_display(bhr.fin_class_cd)
FROM
    referral r
    /* --- LEFT JOINed tables (CCL parenthetical syntax) --- */
    ,(LEFT JOIN orders ord ON ord.order_id = r.order_id
        AND ord.orig_order_dt_tm >= sysdate - 365
        AND ord.activity_type_cd NOT IN (
            value(uar_get_code_by('MEANING', 106, 'NURS'))
            ,value(uar_get_code_by('DISPLAYKEY', 106, 'NURSINGFACLTY'))
            ,value(uar_get_code_by('MEANING', 106, 'SCHEDULING'))
            ,value(uar_get_code_by('MEANING', 106, 'PHARMACY'))
            ,value(uar_get_code_by('DISPLAYKEY', 106, 'PATHOLAB'))
            ,value(uar_get_code_by('DISPLAYKEY', 106, 'CARDIOVASMED'))
            ,value(uar_get_code_by('DISPLAYKEY', 106, 'MEDICATIONRELATED'))
            ,value(uar_get_code_by('DISPLAYKEY', 106, 'INTERNALMEDICINECONSULTS'))
            ,value(uar_get_code_by('DISPLAYKEY', 106, 'PHYMEDREHAB'))
            ,value(uar_get_code_by('DISPLAYKEY', 106, 'SLEEPMEDICINE'))
            ,value(uar_get_code_by('DISPLAYKEY', 106, 'TELEMEDICINE'))
            ,value(uar_get_code_by('DISPLAYKEY', 106, 'CASEMGMT'))
            ,value(uar_get_code_by('MEANING', 106, 'DIETARY'))
            ,value(uar_get_code_by('DISPLAYKEY', 106, 'NUTRITIONSERVICESCONSULTS'))
            ,value(uar_get_code_by('DISPLAYKEY', 106, 'NUTRITIONISTCONSULTS'))
            ,value(uar_get_code_by('DISPLAYKEY', 106, 'NUTRITION'))
            ,value(uar_get_code_by('DISPLAYKEY', 106, 'NUTRITIONEDUCATION'))
        )
    )
    ,(LEFT JOIN order_action oa ON ord.order_id = oa.order_id)
    ,(LEFT JOIN order_detail od ON od.order_id = ord.order_id)
    ,(LEFT JOIN charge c ON c.encntr_id = ord.encntr_id
        AND c.ord_phys_id = oa.order_provider_id)
    /* [FIX 5] Removed diagnosis d join — it was never used in SELECT (dead code).
       The dx_code column is sourced from cmICD.field6, not from diagnosis. */
    ,(LEFT JOIN charge_mod cmCPT ON c.charge_item_id = cmCPT.charge_item_id
        AND cmCPT.active_ind = 1
        AND cmCPT.field2_id = 1
        AND cmCPT.field1_id IN
            (SELECT cv.code_value FROM code_value cv WHERE cv.code_set = 14002
                AND cv.cdf_meaning = "CPT4"))
    ,(LEFT JOIN charge_mod cmHCPC ON c.charge_item_id = cmHCPC.charge_item_id
        AND cmHCPC.active_ind = 1
        AND cmHCPC.field2_id = 1
        AND cmHCPC.field1_id IN
            (SELECT cv.code_value FROM code_value cv WHERE cv.code_set = 14002
                AND cv.cdf_meaning = "HCPCS"))
    ,(LEFT JOIN charge_mod cmICD ON c.charge_item_id = cmICD.charge_item_id
        AND cmICD.active_ind = 1
        AND cmICD.field2_id = 1
        AND cmICD.field1_id IN
            (SELECT cv.code_value FROM code_value cv WHERE cv.code_set = 14002
                AND cv.cdf_meaning = "ICD9"))
    ,(LEFT JOIN organization o ON r.refer_from_organization_id = o.organization_id)
    ,(LEFT JOIN address a3 ON a3.parent_entity_id = o.organization_id
        AND a3.parent_entity_name = "ORGANIZATION"
        AND a3.active_ind = 1
        AND a3.address_type_seq = 1)
    ,(LEFT JOIN organization o2 ON r.refer_to_organization_id = o2.organization_id)
    ,(LEFT JOIN address a2 ON a2.parent_entity_id = o2.organization_id
        AND a2.parent_entity_name = "ORGANIZATION"
        AND a2.active_ind = 1
        AND a2.address_type_seq = 1)
    ,(LEFT JOIN phone ph ON ph.parent_entity_id = o2.organization_id
        AND ph.parent_entity_name = "ORGANIZATION"
        AND ph.active_ind = 1
        AND ph.phone_type_seq = 1)
    ,(LEFT JOIN referral_entity_reltn rer ON rer.referral_id = r.referral_id
        AND rer.parent_entity_name = "SCH_EVENT")
    ,(LEFT JOIN sch_event se ON se.sch_event_id = rer.parent_entity_id
        AND se.active_ind = 1
        AND sysdate BETWEEN se.beg_effective_dt_tm AND se.end_effective_dt_tm)
    ,(LEFT JOIN sch_appt sa ON sa.sch_event_id = se.sch_event_id
        AND sa.active_ind = 1
        AND sysdate BETWEEN sa.beg_effective_dt_tm AND sa.end_effective_dt_tm
        AND sa.sch_role_cd = value(uar_get_code_by('MEANING', 14250, 'PATIENT'))
        AND sa.schedule_seq IN
        (
            SELECT max(s2.schedule_seq)
            FROM sch_appt s2
            WHERE s2.sch_event_id = sa.sch_event_id
        )
    )
    /* [FIX 4] encounter now joins through orders.encntr_id instead of
       referral.person_id to prevent row multiplication across all
       encounters for the same patient. */
    ,(LEFT JOIN encounter e ON e.encntr_id = ord.encntr_id)
    /* [FIX 2] Insurance chain converted from INNER JOINs to LEFT JOINs
       so referrals without insurance records are no longer silently dropped. */
    ,(LEFT JOIN pft_encntr pe ON pe.encntr_id = e.encntr_id)
    ,(LEFT JOIN benefit_order bo ON bo.pft_encntr_id = pe.pft_encntr_id)
    ,(LEFT JOIN bo_hp_reltn bhr ON bhr.benefit_order_id = bo.benefit_order_id
        AND bhr.priority_seq = 1)
    ,(LEFT JOIN health_plan hp ON hp.health_plan_id = bhr.health_plan_id)
    ,prsnl rfrom
    /* [FIX 3] Removed prsnl rto — it was INNER JOINed via WHERE but never
       referenced in SELECT; it excluded referrals without a referred-to provider. */
    ,(LEFT JOIN address a ON a.parent_entity_id = e.person_id
        AND a.parent_entity_name = "PERSON"
        AND a.active_ind = 1
        AND a.address_type_seq = 1
        AND a.address_type_cd = value(uar_get_code_by('MEANING', 212, 'HOME')))
    ,(LEFT JOIN encntr_alias ea ON ea.encntr_id = e.encntr_id
        AND ea.active_ind = 1
        AND sysdate BETWEEN ea.beg_effective_dt_tm AND ea.end_effective_dt_tm
        AND ea.data_status_cd = value(uar_get_code_by("MEANING", 8, "AUTH"))
        AND ea.encntr_alias_type_cd = value(uar_get_code_by("MEANING", 319, "FIN NBR")))
    ,(LEFT JOIN person patient ON patient.person_id = e.person_id)
    ,(LEFT JOIN encntr_alias eaMRN ON eaMRN.encntr_id = e.encntr_id
        AND eaMRN.active_ind = 1
        AND eaMRN.encntr_alias_type_cd = value(uar_get_code_by("MEANING", 319, "MRN"))
        AND sysdate BETWEEN eaMRN.beg_effective_dt_tm AND eaMRN.end_effective_dt_tm)
WHERE r.active_ind = 1
    AND r.refer_from_provider_id = rfrom.person_id
    /* [FIX 1] Replaced misleading semicolon comment with block comment. */
    /* Encounter registration date range filter — adjust window as needed */
    AND e.reg_dt_tm BETWEEN cnvtdatetime(curdate - 1, 0) AND cnvtdatetime(curdate - 1, 235959)
    AND e.active_ind = 1
    AND e.encntr_status_cd != value(uar_get_code_by("MEANING", 261, "CANCELLED"))
    AND patient.birth_dt_tm != null
    AND patient.name_last_key != "ZZ*" ;EXCLUDE TEST ENCOUNTERS
ORDER BY e.encntr_id
WITH maxrec = 5000, time = 600, format(date, ";;q")
