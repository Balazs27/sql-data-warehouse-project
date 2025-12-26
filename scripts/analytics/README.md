# Analytics & SQL Analysis

This folder contains SQL-based analytical work built **on top of the Gold layer**
of the data warehouse.

The analyses demonstrate how the analytics-ready star schema can be consumed
by analysts, BI tools, and stakeholders to generate actionable insights.

---

## Exploratory Data Analysis

**File:** `exploratory_analysis.sql`

This analysis focuses on understanding the structure, coverage, and magnitude
of the data, including:

- Database and schema exploration
- Dimension exploration (customers, products, categories)
- Date range and temporal coverage
- Core business metrics (sales, orders, customers, products)
- Magnitude analysis by country, category, and customer
- Ranking analysis (top/bottom products and customers)

Purpose:
- Validate the data model
- Build intuition about the business
- Identify high-level patterns and distributions

---

## Advanced Analytics

**File:** `advanced_analytics.sql`

This analysis applies more advanced analytical patterns, including:

- Changes-over-time analysis (yearly and monthly trends)
- Cumulative and running totals
- Moving averages
- Year-over-year performance analysis
- Part-to-whole contribution analysis
- Data segmentation (products, customers)
- Customer lifecycle and behavioral segmentation
- KPI construction for reporting and dashboards

The final section builds a reusable **customer reporting view** that can be
shared with other analysts or connected directly to BI tools.

---

## Key Takeaway

These analyses demonstrate how a well-designed data warehouse enables:

- Self-service analytics
- Scalable reporting
- Consistent business logic
- Advanced SQL-based insights without reprocessing raw data
