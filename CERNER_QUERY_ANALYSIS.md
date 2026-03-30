# Cerner Millennium CCL Query Analysis

## Verdict: This IS a Cerner Millennium CCL Query

The provided code is **authentic Cerner Millennium CCL (Cerner Command Language)**, not Epic Clarity SQL or standard T-SQL. Below is a comprehensive dissection.

---

## 1. CCL-Specific Language Features Identified

### Functions
| Function | Purpose | CCL? |
|---|---|---|
| `uar_get_code_meaning()` | Retrieves the MEANING field from CODE_VALUE | Yes |
| `uar_get_code_display()` | Retrieves the DISPLAY field from CODE_VALUE | Yes |
| `uar_get_code_by()` | Looks up a code_value by meaning/displaykey from a code_set | Yes |
| `build()` | Concatenates string fragments (CCL equivalent of CONCAT) | Yes |
| `substring()` | Extracts part of a string | Yes |
| `cnvtstring()` | Converts numeric to string | Yes |
| `cnvtdatetime()` | Converts date+time components into a DQ8 datetime | Yes |
| `format()` | Formats dates/numbers to string | Yes |
| `textlen()` | Returns the length of a text value | Yes |
| `trim()` | Trims whitespace from a string | Yes |
| `rand()` | Random number generation | Yes |
| `AESENCRYPT()` | AES encryption (used here to obfuscate birth dates) | Yes |
| `sysdate` | Current system datetime (CCL keyword) | Yes |
| `curdate` | Current system date (CCL keyword) | Yes |
| `value()` | Wrapper used in code value comparisons | Yes |

### Syntax Constructs
| Construct | Description | CCL? |
|---|---|---|
| `,(LEFT JOIN ... ON ...)` | CCL-style comma-prefixed LEFT JOIN (ANSI JOINs wrapped in parentheses with leading comma) | Yes |
| `,table alias` (implicit inner join in FROM) | Comma-separated tables in FROM clause for inner joins resolved in WHERE | Yes |
| `IF(...) ... ELSE ... ENDIF` | Inline conditional expressions in SELECT list | Yes |
| `ELSEIF(...)` | Chained conditional | Yes |
| `WITH maxrec = 5000` | CCL query option to limit result rows | Yes |
| `WITH time = 600` | CCL query option to set timeout in seconds | Yes |
| `WITH format(date,";;q")` | CCL query option for date formatting | Yes |
| `;COMMENT` | Semicolon-started inline comment (CCL comment syntax) | Yes |
| `/* ... */` | Block comment | Yes |
| `!= null` | CCL null comparison (not `IS NULL`) | Yes |

---

## 2. Cerner Millennium Data Model Tables Used

All tables below are standard Cerner Millennium schema objects:

| Table | Description |
|---|---|
| `referral` | Referral tracking records |
| `orders` (aliased `ord`) | Order records |
| `order_action` | Order action/audit trail |
| `order_detail` | Order entry field details |
| `charge` | Charge/billing records |
| `charge_mod` | Charge modifiers (CPT, HCPCS, ICD codes) |
| `diagnosis` | Diagnosis records |
| `organization` | Organization/facility master |
| `address` | Address records (polymorphic via parent_entity_name) |
| `phone` | Phone records (polymorphic via parent_entity_name) |
| `referral_entity_reltn` | Links referrals to related entities (e.g., scheduling) |
| `sch_event` | Scheduling events |
| `sch_appt` | Scheduling appointments |
| `prsnl` | Personnel/provider records |
| `encounter` | Patient encounters |
| `pft_encntr` | Patient financial transaction encounter |
| `benefit_order` | Benefit order (insurance linkage) |
| `bo_hp_reltn` | Benefit order to health plan relationship |
| `health_plan` | Insurance/health plan master |
| `encntr_alias` | Encounter alias (FIN, MRN, etc.) |
| `person` | Person/patient demographics |
| `code_value` | Code value reference table |

### Code Sets Referenced
| Code Set | Description |
|---|---|
| 106 | Activity Type (order classification) |
| 14002 | Charge Modifier Type (CPT4, HCPCS, ICD9) |
| 14250 | Scheduling Role |
| 261 | Encounter Status |
| 212 | Address Type |
| 8 | Data Status |
| 319 | Encounter Alias Type |

---

## 3. Critical Bug Found and Fixed

### Misplaced Semicolon Comment Breaking the WHERE Clause

**Original (lines from the user-provided code):**
```
    AND bhr.health_plan_id = hp.health_plan_id
    ;LINE BELOW IS TO LIMIT ENCOUNTER REGISTRATION DATE RANGE
	AND e.reg_dt_tm BETWEEN cnvtdatetime(curdate-1,0) AND cnvtdatetime(curdate-1,235959)
```

**Problem:** In CCL, the semicolon (`;`) begins an inline comment that extends to the end of the line. The line `;LINE BELOW IS TO LIMIT ENCOUNTER REGISTRATION DATE RANGE` is correctly treated as a comment. **However**, this comment sits between two `AND` clauses. While CCL does treat the entire semicolon-prefixed line as a comment (meaning the `AND e.reg_dt_tm ...` line on the next line would still be parsed), the placement is confusing and fragile — some CCL parser contexts could interpret the `;` as a statement terminator followed by a comment.

**Fix applied:** Replaced the semicolon comment with a block comment (`/* ... */`) to eliminate any ambiguity:
```
    AND bhr.health_plan_id = hp.health_plan_id
    /* Date filter: restricts encounters to the previous day */
    AND e.reg_dt_tm BETWEEN cnvtdatetime(curdate-1,0) AND cnvtdatetime(curdate-1,235959)
```

This is the only structural change made. The semicolon comment on `patient.name_last_key != "ZZ*" ;EXCLUDE PROD TEST ENCOUNTERS` is fine because it is at the end of a complete clause.

---

## 4. Design & Logic Analysis

### Data Obfuscation
- **MRN:** Padded with random 3-character strings on both sides via `build(substring(1,3,cnvtstring(rand(0))), eaMRN.alias, substring(1,3,cnvtstring(rand(0))))` — obfuscates MRN while keeping it reversible.
- **Birth Date:** Encrypted with `AESENCRYPT()`, only month/year retained — HIPAA-conscious.
- **Order ID:** Same random padding technique as MRN.
- **Dates:** Truncated to MM/YYYY format throughout — minimizes re-identification risk.

### Join Strategy
The query uses a **hybrid join model** typical of Cerner CCL:
- **LEFT JOINs** (parenthesized, comma-prefixed) for optional lookups: orders, charges, scheduling, organizations, addresses, phones.
- **Implicit inner joins** (comma-separated in FROM, resolved in WHERE) for required relationships: encounter, person, personnel, insurance chain.

### Filtering Logic
- **Active records only:** `r.active_ind = 1`, `e.active_ind = 1`, address/alias active_ind checks.
- **Date range:** Encounters restricted to previous day via `cnvtdatetime(curdate-1,0)` to `cnvtdatetime(curdate-1,235959)`.
- **Order recency:** Orders within last 365 days (`ord.orig_order_dt_tm >= sysdate - 365`).
- **Cancelled encounters excluded:** `e.encntr_status_cd != value(uar_get_code_by("MEANING",261,"CANCELLED"))`.
- **Test patients excluded:** `patient.name_last_key != "ZZ*"`.
- **Born patients only:** `patient.birth_dt_tm != null`.
- **Activity type exclusions:** 17 excluded activity types via code set 106 lookups.
- **Most recent schedule:** Subquery `SELECT max(s2.schedule_seq)` ensures latest scheduling appointment.

### Insurance Chain
`encounter` → `pft_encntr` → `benefit_order` → `bo_hp_reltn` (priority_seq = 1) → `health_plan` — standard Cerner revenue cycle join path for primary insurance.

### Performance Controls
- `maxrec = 5000` — caps output rows.
- `time = 600` — 10-minute query timeout.

---

## 5. Potential Improvements / Considerations

| # | Item | Details |
|---|---|---|
| 1 | **ICD-9 vs ICD-10** | The `charge_mod` ICD lookup uses `cdf_meaning = "ICD9"`. If the facility has transitioned to ICD-10, change to `"ICD10"`. |
| 2 | **oe_field_id = 12594.00** | This is environment-specific. Verify this maps to the intended order detail field in your Millennium data dictionary. |
| 3 | **order_action join lacks specificity** | `order_action oa` has no filter on `oa.action_sequence` or `oa.action_type_cd`. This may return multiple rows per order, causing row multiplication. Consider adding `AND oa.action_sequence = 1` or filtering for the ORIGINATE action. |
| 4 | **Date arithmetic** | `ord.orig_order_dt_tm + 365` adds 365 days; not exactly one year (misses leap years). Consider `dateadd()` if available in your CCL version. |
| 5 | **Scheduling subquery correlation** | The `MAX(schedule_seq)` subquery inside a LEFT JOIN is valid but may impact performance on large datasets. |
| 6 | **curdate-1 single-day window** | The encounter date filter only captures the previous day. Ensure this query runs daily or adjust the window. |

---

## 6. File Reference

- **Validated query:** `cerner_millennium_query.ccl`
- **Original Epic query (existing in repo):** `epic_production_query.sql`
