# KIO Site Finder Phase 1 Test Scripts

## Script 1: Freshness Gate Blocks Scoring

**Script ID:** `TS-FRESH-001`

**Objective:** Verify that scoring does not start when a required source is stale.

**Steps**

1. Mark a required source snapshot as older than the allowed cadence for the test metro.
2. Start a new batch and metro score run.
3. Observe run status and score-detail tables.

**SQL Setup**

```sql
update source_snapshot
set snapshot_ts = now() - interval '10 days',
    status = 'success'
where source_id = 'FLOOD'
  and metro_id = 'DFW';
```

**Verification**

```sql
select run_id, status, failure_reason
from score_run
where metro_id = 'DFW'
order by started_at desc
limit 1;

select count(*)
from score_factor_detail
where run_id = (
    select run_id
    from score_run
    where metro_id = 'DFW'
    order by started_at desc
    limit 1
);
```

**Expected Result**

- run status is `failed`
- failure reason indicates stale source
- factor detail count is `0`

## Script 2: Evaluation Creates One Row Per In-Scope Parcel

**Script ID:** `TS-EVAL-001`

**Objective:** Verify evaluation coverage across all in-scope parcels.

**Verification**

```sql
select
    (select count(*)
     from raw_parcels
     where county_fips in ('48113','48121')) as in_scope_parcels,
    (select count(*)
     from parcel_evaluations
     where run_id = :run_id) as evaluation_rows;
```

**Expected Result**

- `evaluation_rows = in_scope_parcels`

## Script 3: Exclusion Event Logging

**Script ID:** `TS-EVAL-002`

**Objective:** Confirm excluded parcels have recorded exclusion events.

**Verification**

```sql
select pe.parcel_id, pe.status, count(ee.exclusion_code) as exclusion_events
from parcel_evaluations pe
left join parcel_exclusion_events ee
  on ee.run_id = pe.run_id
 and ee.parcel_id = pe.parcel_id
where pe.run_id = :run_id
  and pe.status = 'excluded'
group by pe.parcel_id, pe.status
having count(ee.exclusion_code) = 0;
```

**Expected Result**

- query returns zero rows

## Script 4: Factor Cardinality Check

**Script ID:** `TS-SCORE-001`

**Objective:** Verify every scored parcel has exactly ten factor rows.

**Verification**

```sql
select parcel_id
from score_factor_detail
where run_id = :run_id
group by parcel_id
having count(*) <> 10;
```

**Expected Result**

- query returns zero rows

## Script 5: Bonus Cardinality Check

**Script ID:** `TS-SCORE-002`

**Objective:** Verify every scored parcel has exactly five bonus rows.

**Verification**

```sql
select parcel_id
from score_bonus_detail
where run_id = :run_id
group by parcel_id
having count(*) <> 5;
```

**Expected Result**

- query returns zero rows

## Script 6: Direct Evidence Overrides Proxy

**Script ID:** `TS-SCORE-003`

**Objective:** Confirm scoring prefers direct evidence when both direct and proxy inputs exist.

**Steps**

1. Seed a parcel with conflicting direct and proxy evidence for the same factor.
2. Execute scoring for the parcel.
3. Inspect factor detail and provenance.

**Verification**

```sql
select factor_id, points_awarded, rationale
from score_factor_detail
where run_id = :run_id
  and parcel_id = :parcel_id
  and factor_id = 'F02';

select input_name, input_value, evidence_quality
from score_factor_input
where run_id = :run_id
  and parcel_id = :parcel_id
  and factor_id = 'F02'
order by input_name;
```

**Expected Result**

- rationale indicates direct evidence was selected
- proxy input may be recorded but does not override direct evidence

## Script 7: No Pending Rows on Completed Run

**Script ID:** `TS-BATCH-001`

**Objective:** Ensure completed runs leave no pending rows behind.

**Verification**

```sql
select count(*)
from parcel_evaluations
where run_id = :run_id
  and status in ('pending_exclusion_check', 'pending_scoring');
```

**Expected Result**

- count is `0`

## Script 8: Batch Activation Completeness

**Script ID:** `TS-BATCH-002`

**Objective:** Ensure a batch activates only after all metros succeed.

**Verification**

```sql
select batch_id, status, expected_metros, completed_metros, activated_at
from score_batch
where batch_id = :batch_id;

select metro_id, status
from score_run
where batch_id = :batch_id
order by metro_id;
```

**Expected Result**

- batch is active only when `completed_metros = expected_metros`
- no required metro run has status `failed`

## Script 9: Active-Batch Read Isolation

**Script ID:** `TS-BATCH-003`

**Objective:** Ensure read APIs stay on the prior active batch until the new batch is activated.

**API Call**

```http
GET /api/v1/batches/active
GET /api/v1/parcels?metro=DFW&minScore=70
```

**Expected Result**

- active batch ID remains unchanged while a new batch is still building
- parcel search results resolve only against that active batch ID

## Script 10: Duplicate Provenance Protection on Retry

**Script ID:** `TS-OPS-001`

**Objective:** Confirm retries do not create duplicate factor inputs.

**Steps**

1. Execute scoring for a run.
2. Retry the same run after a controlled failure or replay.
3. Compare provenance counts before and after.

**Verification**

```sql
select parcel_id, factor_id, input_name, count(*)
from score_factor_input
where run_id = :run_id
group by parcel_id, factor_id, input_name
having count(*) > 1;
```

**Expected Result**

- query returns zero rows

## Script 11: Reader Role Cannot Perform Admin Action

**Script ID:** `TS-SEC-001`

**Objective:** Verify role-based access control for admin endpoints.

**API Call**

```http
POST /api/v1/admin/batches/{batchId}/activate
Authorization: Bearer <reader-token>
```

**Expected Result**

- response is `403 Forbidden`
- security audit log captures the denied attempt

## Script 12: Launch Readiness Smoke Script

**Script ID:** `TS-SMOKE-001`

**Objective:** Execute a final smoke check before cutover.

**Checklist**

1. Confirm all required sources show fresh status.
2. Start a controlled batch for pilot metros.
3. Confirm all score runs complete successfully.
4. Confirm factor and bonus cardinality checks pass.
5. Activate the batch.
6. Confirm parcel search and detail APIs respond successfully.
7. Export a sample audit report for one parcel.

**Expected Result**

- the platform is ready for production cutover with one valid active batch
