## 1. Pipeline Ownership

We have 5 pipelines, and we’re 4 engineers. Best practice is:

Each pipeline has a primary owner (responsible for design, improvements, and responding first if issues occur).

Each pipeline also has a secondary owner (backup, reviewer, and helps if the primary is unavailable).

Here’s a fair allocation:

Pipeline	Primary Owner	Secondary Owner
Profit – Unit-level (experiments)	Engineer A	Engineer B
Profit – Aggregate (investors)	Engineer B	Engineer C
Growth – Aggregate (investors)	Engineer C	Engineer D
Growth – Daily (experiments)	Engineer D	Engineer A
Engagement – Aggregate (investors)	Engineer A	Engineer C

💡 Notice:

Investor-facing pipelines (aggregate metrics) are spread across different primaries.

Experimental pipelines are balanced as “lighter” ownership since they’re less investor-sensitive.

## 2. On-Call Schedule (Fair & Holidays Considered)

We want a weekly rotation with clear backup. For 4 engineers:

- Week 1: Engineer A (primary on-call), Engineer B (backup)

- Week 2: Engineer B (primary on-call), Engineer C (backup)

- Week 3: Engineer C (primary on-call), Engineer D (backup)

- Week 4: Engineer D (primary on-call), Engineer A (backup)

Then repeat.

**Holidays**

If someone is on vacation/holiday, they swap weeks in advance.

Backup always covers if primary is offline unexpectedly.

Investor-reporting pipelines (Profit agg, Growth agg, Engagement agg) have escalation paths to ensure issues are handled even if both are out (escalate to Eng. Manager).

## 3. Runbooks for Investor-Facing Pipelines

These are critical because executives and investors depend on them.
Pipelines:

- Profit – Aggregate

- Growth – Aggregate

- Engagement – Aggregate

**General Runbook Template**

- Pipeline Purpose: What business metric is being calculated.

- Data Sources: Which raw data tables and external APIs feed it.

- Transformations: Key joins, filters, aggregations.

- SLAs: By what time data must be ready (e.g., 8am UTC daily).

**Failure Modes (what can go wrong).**

- Recovery Steps: How to troubleshoot and fix.

- Escalation: Who to notify if SLA is missed.

# Examples: 

**Profit – Aggregate Runbook**

- Purpose: Compute total company profit daily/monthly for investor reports.
Sources:

- Transactions table (orders, refunds).

- Cost data (fulfillment, returns, platform fees).
*Transformations:*

- Profit = Revenue – Costs (per transaction).

- Aggregate by day, month, quarter.

*Failure Modes:*

- Transaction data delayed → profit under-reported.

- Cost data missing for certain regions → inflated profit.

- Schema changes in source tables.

- Late-arriving data from external systems (e.g., refunds after cutoff).

*Recovery Steps:*

- Check DAG logs (Airflow/DBT/etc.).

- Re-run only failed tasks if DAG is partially complete.

- If data missing from source → notify source system team.

- Validate aggregates vs. last known good run.

*Escalation:*

- Primary → Secondary → Data Eng. Manager if unresolved in 2 hours.

**Growth – Aggregate Runbook**

- Purpose: Track new users/customers daily, roll up to monthly growth rate.
*Failure Modes:*

- Duplicate user IDs inflating growth.

- User acquisition events missing.

- Late ingestion from marketing APIs.

*Recovery:*

- Reconcile user IDs vs. historical records.

- Retry API pulls.

- Apply deduplication.

**Engagement – Aggregate Runbook**

- Purpose: Active users (DAU, MAU) and engagement metrics.
Failure Modes:

- Missing event logs from tracking system (Kafka/Segment/etc.).

- Spikes in data due to bad instrumentation.

- Event schema change (e.g., “login” → “user_login”).

*Recovery:*

- Verify ingestion status in logs.

- Cross-check user counts with sample queries.

- Roll back to previous schema mappings if needed.

## Conclusion:
With this, every engineer knows:

- Who owns each pipeline.

- When they’re on-call (and with backup).

- What to do when investor-facing pipelines break.  












#####

# 📘 Data Pipeline Runbooks

This document contains runbooks for all **investor-facing data pipelines**:

- [Profit – Aggregate](#-profit--aggregate-pipeline)  
- [Growth – Aggregate](#-growth--aggregate-pipeline)  
- [Engagement – Aggregate](#-engagement--aggregate-pipeline)  

Each runbook includes **purpose, SLA, owners, architecture, failure modes, monitoring, troubleshooting, recovery, escalation, and validation queries**.

---

## 💰 Profit – Aggregate Pipeline

### 1. Overview
- **Pipeline Name:** Profit – Aggregate  
- **Business Purpose:** Calculates **total company profit** (Revenue – Costs) at a daily, monthly, and quarterly level. Metrics are used in **investor reports** and **executive dashboards**.  
- **SLA:**  
  - Daily run by **08:00 UTC**  
  - Monthly/quarterly by **12:00 UTC** (first business day after close)  
- **Owners:**  
  - Primary → **Engineer B**  
  - Secondary → **Engineer C**  
- **Dependencies:**  
  - Transactions DB (orders, refunds)  
  - Cost data (fulfillment, fees)  
  - Data warehouse  

---

### 2. Architecture
- **Workflow:** Airflow (orchestration)  
- **Processing:** Spark + dbt  
- **Storage:** S3 → Snowflake (or equivalent)  
- **Dashboards:** Tableau / Looker  

**Flow:**  
1. Extract orders & costs  
2. Load into staging  
3. Transform into facts (`fact_orders`, `fact_costs`)  
4. Compute unit-level profit  
5. Aggregate → daily/monthly/quarterly  

---

### 3. Potential Failure Modes
| Failure | Symptom | Cause | Risk |
|---------|---------|-------|------|
| Missing transaction data | Profit lower than expected | ETL/replication lag | Under-reporting |
| Missing cost data | Profit inflated | Cost pipeline delay | Over-reporting |
| Schema change | Job failure | Source renamed/changed | Pipeline breakage |
| Late-arriving data | Inconsistent history | Refunds posted late | Misleading profit |
| Data skew | Long runtime | Poor partitioning | SLA breach |

---

### 4. Monitoring & Alerts
- Airflow DAG alerts → Slack `#data-eng-alerts`  
- dbt tests: no NULLs, profit sanity check  
- Dashboard anomaly detection → alert if deviation >20%  

---

### 5. Troubleshooting
#### Case 1: Pipeline Failed
- Check Airflow logs  
- Retry failed task  
- If schema mismatch → hotfix dbt model  

#### Case 2: Missing Data
```sql
SELECT COUNT(*) 
FROM fact_orders
WHERE order_date = CURRENT_DATE;
```

- Compare with historical averages

#### Case 3: SLA Breach

- Enable Spark dynamic allocation

- Re-run failed partition

### 6. Recovery
```sql
DELETE FROM agg_profit_daily WHERE order_date = '2025-09-18';
```
Re-run for specific date and validate counts.

### 7. Escalation

- Primary → Secondary → Eng. Manager (2 hrs) → Finance Lead (6 hrs if investor impact)

### 8. Validation Queries
```sql
-- Profit sanity check
SELECT SUM(revenue), SUM(cost), SUM(profit)
FROM agg_profit_daily
WHERE order_date = CURRENT_DATE;

-- Compare vs yesterday
SELECT a.order_date, a.total_profit, b.total_profit AS yesterday_profit,
       ROUND((a.total_profit - b.total_profit)*100.0 / b.total_profit, 2) AS pct_diff
FROM agg_profit_daily a
JOIN agg_profit_daily b
  ON a.order_date = CURRENT_DATE
 AND b.order_date = CURRENT_DATE - INTERVAL '1 day';
 ```


## 📈 Growth – Aggregate Pipeline
### 1. Overview

Pipeline Name: Growth – Aggregate

Business Purpose: Computes user/customer growth (new, churned, net).

SLA:

Daily by 08:30 UTC

Monthly by 12:00 UTC (first business day)

Owners:

Primary → Engineer C

Secondary → Engineer D

Dependencies: Users table, marketing APIs, churn logs

### 2. Architecture

Extract registrations + churn

Pull marketing data (API)

Staging → facts (fact_growth)

Aggregates (agg_growth)

### 3. Potential Failure Modes
| Failure         | Symptom                | Cause                 | Risk                   |
| --------------- | ---------------------- | --------------------- | ---------------------- |
| Duplicate users | Inflated growth        | Multi-channel signups | Over-reporting         |
| API failures    | Missing marketing data | Rate limit / outage   | Incomplete attribution |
| Late churn      | Net growth inflated    | Lag in pipeline       | Misleading             |
| Schema drift    | Join failure           | API field change      | Breakage               |

### 4. Monitoring & Alerts

Airflow SLA alerts

dbt tests: unique user_id, no extreme negatives

Deviation >25% from 7-day average → alert

### 5. Troubleshooting
#### Case 1: API Pull Failed

Check API logs

Retry smaller batches

Contact Marketing Ops

#### Case 2: Duplicate Users
```sql
SELECT user_id, COUNT(*)
FROM users
GROUP BY user_id
HAVING COUNT(*) > 1;
```
Deduplicate on first_seen_date

#### Case 3: Churn Lag

Compare ingestion timestamps

Trigger incremental reload

### 6. Recovery
```sql
DELETE FROM agg_growth WHERE date = '2025-09-18';
```
Re-run ingestion + transformations.

### 7. Escalation

Primary → Secondary → Eng. Manager → Growth Analytics Lead

### 8. Validation Queries
```sql
-- Net growth check
SELECT SUM(new_users) - SUM(churned_users) AS net_growth
FROM agg_growth
WHERE date = CURRENT_DATE;

-- Compare vs yesterday
SELECT today.date, today.net_growth, yesterday.net_growth,
       ROUND((today.net_growth - yesterday.net_growth)*100.0 / yesterday.net_growth, 2) AS pct_diff
FROM (
    SELECT date, SUM(new_users)-SUM(churned_users) AS net_growth
    FROM agg_growth
    WHERE date = CURRENT_DATE
    GROUP BY date
) today
JOIN (
    SELECT date, SUM(new_users)-SUM(churned_users) AS net_growth
    FROM agg_growth
    WHERE date = CURRENT_DATE - INTERVAL '1 day'
    GROUP BY date
) yesterday ON 1=1;

```

## 📊 Engagement – Aggregate Pipeline
### 1. Overview

Pipeline Name: Engagement – Aggregate

Business Purpose: Computes DAU, WAU, MAU, retention.

SLA:

Daily by 09:00 UTC

Monthly MAU by 12:00 UTC (month close)

Owners:

Primary → Engineer A

Secondary → Engineer C

Dependencies: Event logs (Kafka/S3), session data

### 2. Architecture

Ingest raw events

Transform into facts (fact_sessions, fact_events)

Aggregates (agg_engagement)

### 3. Potential Failure Modes
| Failure          | Symptom       | Cause                | Risk            |
| ---------------- | ------------- | -------------------- | --------------- |
| Ingestion lag    | DAU drops     | Kafka consumer stuck | Under-reporting |
| Duplicate events | DAU inflated  | Instrumentation bug  | Over-reporting  |
| Schema drift     | Broken fields | SDK change           | Breakage        |
| Bot traffic      | DAU inflated  | Bad traffic          | Misleading      |

### 4. Monitoring & Alerts

Kafka consumer lag

dbt tests: DAU > 0, user_id not NULL

DAU deviation >30% from 14-day average → alert

### 5. Troubleshooting
#### Case 1: Ingestion Lag

Check Kafka offsets

Restart consumer

#### Case 2: Duplicate Events

```sql
SELECT user_id, event_time, COUNT(*)
FROM fact_events
GROUP BY user_id, event_time
HAVING COUNT(*) > 1;

```
Deduplicate on user_id, session_id, event_type

#### Case 3: Bot Traffic

Identify spikes by IP/device

Exclude via filters

### 6. Recovery 
```sql
DELETE FROM agg_engagement WHERE session_date = '2025-09-18';

```

Reprocess raw events for affected dates.

### 7. Escalation

Primary → Secondary → Eng. Manager → Product Analytics Lead

### 8. Validation Queries
```sql
-- DAU check
SELECT COUNT(DISTINCT user_id) AS dau
FROM fact_sessions
WHERE session_date = CURRENT_DATE;

-- MAU check
SELECT COUNT(DISTINCT user_id) AS mau
FROM fact_sessions
WHERE session_date >= DATE_TRUNC('month', CURRENT_DATE);


```