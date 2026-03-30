/*
-----------------------------------------------------------------------------------------------------------------
-- Abax Healthcare, Inc
-- Contact: Jay Dalke
-- Modified: 03/30/2026
-- Email: support@abaxhealth.com
-- Please reach out to Jay Dalke for any assistance with this query. 
-- Query support is part of Abax Health's comprehensive implementation process.
-----------------------------------------------------------------------------------------------------------------
-- Purpose:
-- This query extracts detailed patient, order, and referral information for Abax Health
-- from a Cerner Millennium environment using CCL (Cerner Command Language).
-- It joins referral, order, encounter, scheduling, charge, and insurance data
-- for comprehensive reporting on referral activity.
-----------------------------------------------------------------------------------------------------------------
-- Platform: Cerner Millennium (CCL)
-----------------------------------------------------------------------------------------------------------------
-- Filters:
-- @DATE FILTER (Orders)
--    Orders are limited to the past 365 days via ord.orig_order_dt_tm >= sysdate - 365.
-- @ENCOUNTER DATE FILTER
--    Encounters are filtered to the prior day's registrations.
--    Adjust cnvtdatetime(curdate-1,...) range as needed.
-- @ACTIVITY TYPE FILTER
--    Excludes order activity types not in scope (nursing, pharmacy, dietary, etc.).
-- @TEST PATIENT FILTER
--    Excludes test patients whose name_last_key begins with "ZZ".
-- @DECEASED FILTER
--    Excludes patients with a null birth date (birth_dt_tm != null).
-- @ENCOUNTER STATUS FILTER
--    Excludes cancelled encounters.
-----------------------------------------------------------------------------------------------------------------
-- Key Tables:
--   referral               - Core referral records
--   orders                 - Order data linked to referrals
--   order_action           - Provider actions on orders
--   order_detail           - Order detail fields (e.g. diagnosis OE field)
--   charge / charge_mod    - Charge and modifier data (CPT, HCPCS, ICD codes)
--   diagnosis              - Encounter diagnosis records
--   organization / address - Referring-from and referring-to facility info
--   phone                  - Organization phone numbers
--   sch_event / sch_appt   - Scheduling event and appointment data
--   encounter              - Patient encounter records
--   encntr_alias           - FIN and MRN aliases
--   person                 - Patient demographics
--   pft_encntr / benefit_order / bo_hp_reltn / health_plan - Insurance data
--   prsnl                  - Provider/personnel records
--   code_value             - Cerner code value reference table
-----------------------------------------------------------------------------------------------------------------
-- REVIEW NOTES (issues identified and addressed):
--
-- 1. CRITICAL FIX: Semicolon comment splitting the WHERE clause
--    Original had ";LINE BELOW IS TO LIMIT ENCOUNTER REGISTRATION DATE RANGE"
--    between two AND conditions. In CCL, ";" starts a line comment, so the text
--    after the semicolon on that line is correctly treated as a comment. However,
--    this placement is extremely misleading — it visually appears to break the
--    WHERE clause. Reformatted as a block of commented-out section dividers for
--    clarity.
--
-- 2. ICD CODE VERSION: The charge_mod join for diagnosis codes filters on
--    cdf_meaning = "ICD9". ICD-10 has been the US standard since October 2015.
--    Updated to "ICD10" with a note to revert to "ICD9" only if the site still
--    uses ICD-9 mappings in charge_mod.
--
-- 3. DUPLICATE RISK: order_action (oa) is LEFT JOINed without filtering to a
--    specific action_sequence. Multiple actions per order can cause row
--    duplication. Added action_sequence = 1 filter to restrict to the originating
--    action.
--
-- 4. PERFORMANCE NOTE: Several tables (prsnl, encounter, pft_encntr,
--    benefit_order, bo_hp_reltn, health_plan, address, encntr_alias, person)
--    use old-style comma-separated implicit INNER JOINs filtered in the WHERE
--    clause. This is valid CCL but creates Cartesian products before filtering.
--    No structural change was made since this is a common CCL pattern, but
--    explicit JOINs would improve readability and optimizer hints.
--
-- 5. FORMATTING: Standardized indentation, aligned column aliases, and added
--    section dividers for readability.
--
-- 6. order_detail (od) join: od is joined unfiltered, then the diagnosis join
--    filters on od.oe_field_id = 12594.00. Moved the oe_field_id filter into
--    the od LEFT JOIN condition for clarity and to reduce intermediate row count.
--
-- 7. MISSING bhr.active_ind FILTER: Added active_ind = 1 check on bo_hp_reltn
--    to exclude inactive insurance records.
-----------------------------------------------------------------------------------------------------------------
*/

SELECT DISTINCT
     id                                = ea.alias

    ,patient_mrn                       = build(
                                            substring(1,3,cnvtstring(rand(0)))
                                           ,eaMRN.alias
                                           ,substring(1,3,cnvtstring(rand(0)))
                                         )

    ,patient_gender                    = uar_get_code_meaning(patient.sex_cd)

    ,patient_city                      = a.city
    ,patient_state                     = uar_get_code_display(a.state_cd)
    ,patient_zip                       = a.zipcode_key

    ,patient_birthdate                 = AESENCRYPT(
                                            1
                                           ,format(patient.birth_dt_tm,"MM/YYYY;;d")
                                           ,textlen(format(patient.birth_dt_tm,"MM/YYYY;;d"))
                                           ,0
                                         )

    ,order_id                          = build(
                                            substring(1,3,cnvtstring(rand(0)))
                                           ,cnvtstring(r.order_id)
                                           ,substring(1,3,cnvtstring(rand(0)))
                                         )

    ,order_date                        = format(ord.orig_order_dt_tm,"MM/YYYY;;d")

    ,order_proc_code                   = IF(cmCPT.field6 != null)
                                            cmCPT.field6
                                         ELSE
                                            cmHCPC.field6
                                         ENDIF

    ,order_proc_code_description       = c.charge_description

    ,referral_status_cd                = uar_get_code_display(r.referral_status_cd)
    ,order_status_cd                   = uar_get_code_display(ord.order_status_cd)

    ,order_expiration_date             = IF(ord.projected_stop_dt_tm != null)
                                            format(ord.projected_stop_dt_tm,"MM/YYYY;;d")
                                         ELSEIF(ord.orig_order_dt_tm != null)
                                            format(ord.orig_order_dt_tm + 365,"MM/YYYY;;d")
                                         ENDIF

    ,ordering_physician_name           = trim(rfrom.name_full_formatted)

    ,referral_class                    = uar_get_code_display(ord.activity_type_cd)
    ,referral_id                       = r.referral_id

    ,referred_to_location              = o2.org_name
    ,referred_to_location_address      = a2.street_addr
    ,referred_to_location_city         = a2.city
    ,referred_to_location_department   = uar_get_code_display(ord.catalog_type_cd)
    ,referred_to_location_state        = uar_get_code_display(a2.state_cd)
    ,referred_to_location_zip          = a2.zipcode_key
    ,referred_to_location_phone        = ph.phone_num_key

    ,cpt_code                          = cmCPT.field6
    ,department_category               = o.org_name
    ,dx_code                           = cmICD.field6

    ,location                          = uar_get_code_display(r.refer_from_loc_cd)
    ,location_address                  = a3.street_addr
    ,location_city                     = a3.city
    ,location_state                    = uar_get_code_display(a3.state_cd)
    ,location_zip                      = a3.zipcode_key

    ,admit_date                        = format(e.reg_dt_tm,"MM/YYYY")
    ,schedule_date                     = format(sa.beg_dt_tm,"MM/YYYY;;d")
    ,scheduling_status                 = uar_get_code_display(sa.sch_state_cd)

    ,insurance1_payer_name             = hp.plan_name
    ,insurance1_financial_class        = uar_get_code_display(bhr.fin_class_cd)

FROM
    referral r

    /* --- Orders (past 365 days, excluding out-of-scope activity types) --- */
    ,(LEFT JOIN orders ord ON ord.order_id = r.order_id
        AND ord.orig_order_dt_tm >= sysdate - 365
        AND ord.activity_type_cd NOT IN (
             value(uar_get_code_by('MEANING',106,'NURS'))
            ,value(uar_get_code_by('DISPLAYKEY',106,'NURSINGFACLTY'))
            ,value(uar_get_code_by('MEANING',106,'SCHEDULING'))
            ,value(uar_get_code_by('MEANING',106,'PHARMACY'))
            ,value(uar_get_code_by('DISPLAYKEY',106,'PATHOLAB'))
            ,value(uar_get_code_by('DISPLAYKEY',106,'CARDIOVASMED'))
            ,value(uar_get_code_by('DISPLAYKEY',106,'MEDICATIONRELATED'))
            ,value(uar_get_code_by('DISPLAYKEY',106,'INTERNALMEDICINECONSULTS'))
            ,value(uar_get_code_by('DISPLAYKEY',106,'PHYMEDREHAB'))
            ,value(uar_get_code_by('DISPLAYKEY',106,'SLEEPMEDICINE'))
            ,value(uar_get_code_by('DISPLAYKEY',106,'TELEMEDICINE'))
            ,value(uar_get_code_by('DISPLAYKEY',106,'CASEMGMT'))
            ,value(uar_get_code_by('MEANING',106,'DIETARY'))
            ,value(uar_get_code_by('DISPLAYKEY',106,'NUTRITIONSERVICESCONSULTS'))
            ,value(uar_get_code_by('DISPLAYKEY',106,'NUTRITIONISTCONSULTS'))
            ,value(uar_get_code_by('DISPLAYKEY',106,'NUTRITION'))
            ,value(uar_get_code_by('DISPLAYKEY',106,'NUTRITIONEDUCATION'))
        )
    )

    /* --- Order action (restrict to originating action to prevent duplicates) --- */
    ,(LEFT JOIN order_action oa ON ord.order_id = oa.order_id
        AND oa.action_sequence = 1)

    /* --- Order detail (diagnosis OE field only) --- */
    ,(LEFT JOIN order_detail od ON od.order_id = ord.order_id
        AND od.oe_field_id = 12594.00)

    /* --- Charge --- */
    ,(LEFT JOIN charge c ON c.encntr_id = ord.encntr_id
        AND c.ord_phys_id = oa.order_provider_id)

    /* --- Diagnosis (matched via order detail display value) --- */
    ,(LEFT JOIN diagnosis d ON d.encntr_id = c.encntr_id
        AND d.diag_prsnl_id = c.ord_phys_id
        AND d.diagnosis_display = od.oe_field_display_value)

    /* --- Charge modifiers: CPT --- */
    ,(LEFT JOIN charge_mod cmCPT ON c.charge_item_id = cmCPT.charge_item_id
        AND cmCPT.active_ind = 1
        AND cmCPT.field2_id = 1
        AND cmCPT.field1_id IN
            (SELECT cv.code_value FROM code_value cv
              WHERE cv.code_set = 14002
                AND cv.cdf_meaning = "CPT4"))

    /* --- Charge modifiers: HCPCS --- */
    ,(LEFT JOIN charge_mod cmHCPC ON c.charge_item_id = cmHCPC.charge_item_id
        AND cmHCPC.active_ind = 1
        AND cmHCPC.field2_id = 1
        AND cmHCPC.field1_id IN
            (SELECT cv.code_value FROM code_value cv
              WHERE cv.code_set = 14002
                AND cv.cdf_meaning = "HCPCS"))

    /* --- Charge modifiers: ICD (updated to ICD10; change to "ICD9" if site uses ICD-9) --- */
    ,(LEFT JOIN charge_mod cmICD ON c.charge_item_id = cmICD.charge_item_id
        AND cmICD.active_ind = 1
        AND cmICD.field2_id = 1
        AND cmICD.field1_id IN
            (SELECT cv.code_value FROM code_value cv
              WHERE cv.code_set = 14002
                AND cv.cdf_meaning = "ICD10"))

    /* --- Referring-from organization and address --- */
    ,(LEFT JOIN organization o ON r.refer_from_organization_id = o.organization_id)
    ,(LEFT JOIN address a3 ON a3.parent_entity_id = o.organization_id
        AND a3.parent_entity_name = "ORGANIZATION"
        AND a3.active_ind = 1
        AND a3.address_type_seq = 1)

    /* --- Referred-to organization, address, and phone --- */
    ,(LEFT JOIN organization o2 ON r.refer_to_organization_id = o2.organization_id)
    ,(LEFT JOIN address a2 ON a2.parent_entity_id = o2.organization_id
        AND a2.parent_entity_name = "ORGANIZATION"
        AND a2.active_ind = 1
        AND a2.address_type_seq = 1)
    ,(LEFT JOIN phone ph ON ph.parent_entity_id = o2.organization_id
        AND ph.parent_entity_name = "ORGANIZATION"
        AND ph.active_ind = 1
        AND ph.phone_type_seq = 1)

    /* --- Scheduling (referral -> sch_event -> sch_appt) --- */
    ,(LEFT JOIN referral_entity_reltn rer ON rer.referral_id = r.referral_id
        AND rer.parent_entity_name = "SCH_EVENT")
    ,(LEFT JOIN sch_event se ON se.sch_event_id = rer.parent_entity_id
        AND se.active_ind = 1
        AND sysdate BETWEEN se.beg_effective_dt_tm AND se.end_effective_dt_tm)
    ,(LEFT JOIN sch_appt sa ON sa.sch_event_id = se.sch_event_id
        AND sa.active_ind = 1
        AND sysdate BETWEEN sa.beg_effective_dt_tm AND sa.end_effective_dt_tm
        AND sa.sch_role_cd = value(uar_get_code_by('MEANING',14250,'PATIENT'))
        AND sa.schedule_seq IN
        (
            SELECT max(s2.schedule_seq)
            FROM sch_appt s2
            WHERE s2.sch_event_id = sa.sch_event_id
        )
    )

    /* --- Implicit INNER JOINs (filtered in WHERE clause) --- */
    ,prsnl           rfrom
    ,prsnl           rto
    ,encounter       e
    ,pft_encntr      pe
    ,benefit_order   bo
    ,bo_hp_reltn     bhr
    ,health_plan     hp
    ,address         a
    ,encntr_alias    ea
    ,person          patient
    ,encntr_alias    eaMRN

WHERE r.active_ind = 1

    /* --- Provider links --- */
    AND r.refer_from_provider_id = rfrom.person_id
    AND r.refer_to_provider_id   = rto.person_id

    /* --- Encounter --- */
    AND r.person_id  = e.person_id
    AND e.active_ind = 1
    AND e.encntr_status_cd != value(uar_get_code_by("MEANING",261,"CANCELLED"))

    /* --- @ENCOUNTER DATE FILTER (prior day registrations; adjust range as needed) --- */
    AND e.reg_dt_tm BETWEEN cnvtdatetime(curdate-1,0) AND cnvtdatetime(curdate-1,235959)

    /* --- Insurance chain --- */
    AND pe.encntr_id       = e.encntr_id
    AND pe.pft_encntr_id   = bo.pft_encntr_id
    AND bo.benefit_order_id = bhr.benefit_order_id
    AND bhr.priority_seq   = 1
    AND bhr.active_ind     = 1
    AND bhr.health_plan_id = hp.health_plan_id

    /* --- Patient home address --- */
    AND a.parent_entity_id   = e.person_id
    AND a.parent_entity_name = "PERSON"
    AND a.active_ind         = 1
    AND a.address_type_seq   = 1
    AND a.address_type_cd    = value(uar_get_code_by('MEANING',212,'HOME'))

    /* --- Encounter alias: FIN NBR --- */
    AND e.encntr_id       = ea.encntr_id
    AND ea.active_ind     = 1
    AND sysdate BETWEEN ea.beg_effective_dt_tm AND ea.end_effective_dt_tm
    AND ea.data_status_cd          = value(uar_get_code_by("MEANING",8,"AUTH"))
    AND ea.encntr_alias_type_cd    = value(uar_get_code_by("MEANING",319,"FIN NBR"))

    /* --- Patient demographics --- */
    AND e.person_id          = patient.person_id
    AND patient.birth_dt_tm != null
    AND patient.name_last_key != "ZZ*"   ;exclude test patients

    /* --- Encounter alias: MRN --- */
    AND e.encntr_id             = eaMRN.encntr_id
    AND eaMRN.active_ind        = 1
    AND eaMRN.encntr_alias_type_cd = value(uar_get_code_by("MEANING",319,"MRN"))
    AND sysdate BETWEEN eaMRN.beg_effective_dt_tm AND eaMRN.end_effective_dt_tm

ORDER BY e.encntr_id

WITH maxrec = 5000, time = 600, format(date,";;q")
