# B2B Marketing Pipeline Analyzer

A Python + SQL tool that connects marketing spend data to sales pipeline outcomes. Built to answer the questions B2B growth teams actually care about: which channels drive real revenue, what's the true cost of acquiring a customer, and where should budget go next quarter.

## What it does

- Loads marketing spend and CRM deal data into an in-memory SQLite database
- Runs SQL queries to calculate channel-level ROAS, CAC, and pipeline contribution
- Breaks down deal performance by customer segment (SMB / mid-market / enterprise)
- Ranks campaigns by closed-won revenue
- Generates actionable budget recommendations based on channel performance

## Example output

```
======================================================================
CHANNEL PERFORMANCE (H1 2025)
======================================================================

channel          total_spend  total_leads  deals_won  revenue_won    cac     roas
email                  7200          196          4       180000    1800    25.0
content_syndication   58200          340          4       177000   14550     3.04
linkedin_ads         114500          283          4       221000   28625     1.93
events               115000          297          2       195000   57500     1.70
...
```

## Setup

```bash
pip install -r requirements.txt
python pipeline_analyzer.py
```

## How it works

1. **Data ingestion**: Reads CSV files (marketing spend + pipeline deals) and loads them into SQLite
2. **SQL analysis**: Joins spend data to deal outcomes by channel and month to calculate ROI metrics
3. **Segmentation**: Breaks down win rates and deal sizes by customer segment
4. **Recommendations**: Applies simple rules (ROAS thresholds) to flag channels for budget increases, optimisation, or cuts

## Data

Sample data is included in `/data`. In a production setting, you'd connect this to your CRM (Salesforce, HubSpot) and marketing platform exports.

| File | Description |
|------|-------------|
| `marketing_spend.csv` | Monthly channel-level spend, impressions, clicks, and leads |
| `pipeline_deals.csv` | Individual deal records with source attribution, stage, value, and segment |

## Tech

- **Python** — pandas for data manipulation
- **SQL** (SQLite) — all core analysis logic runs as SQL queries
- **No external APIs** — runs entirely on local data

## Why this exists

I built this to practise the kind of analysis a B2B growth team runs regularly: connecting top-of-funnel marketing spend to bottom-of-funnel revenue, and using the results to make budget allocation decisions. The SQL is deliberately front-and-centre because that's the day-to-day tool for this kind of work.

## Other projects

- [influencer-marketing-report](https://github.com/anecdotal-trout/influencer-marketing-report) — Influencer campaign ROI analysis
- [saas-growth-dashboard](https://github.com/anecdotal-trout/saas-growth-dashboard) — SaaS growth metrics tracker
