"""
B2B Marketing Pipeline Analyzer
================================
Connects marketing spend data to pipeline outcomes to answer:
- Which channels generate the most pipeline per dollar spent?
- What's the CAC by channel and segment?
- Where should we increase/decrease spend next quarter?

Uses SQLite for SQL-based analysis and pandas for data manipulation.
"""

import sqlite3
import pandas as pd
import os

# ---------------------------------------------------------------------------
# 1. LOAD DATA INTO SQLITE
# ---------------------------------------------------------------------------

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
DB_PATH = ":memory:"  # in-memory database; swap to a file path if needed

def load_data():
    """Read CSVs into pandas, then load into a SQLite database."""
    spend = pd.read_csv(os.path.join(DATA_DIR, "marketing_spend.csv"), parse_dates=["date"])
    deals = pd.read_csv(os.path.join(DATA_DIR, "pipeline_deals.csv"), parse_dates=["created_date", "close_date"])

    conn = sqlite3.connect(DB_PATH)
    spend.to_sql("marketing_spend", conn, if_exists="replace", index=False)
    deals.to_sql("pipeline_deals", conn, if_exists="replace", index=False)

    return conn


# ---------------------------------------------------------------------------
# 2. SQL QUERIES — channel performance, CAC, segment breakdown
# ---------------------------------------------------------------------------

CHANNEL_PERFORMANCE_SQL = """
    SELECT
        s.channel,
        ROUND(SUM(s.spend_usd), 0)                         AS total_spend,
        SUM(s.leads_generated)                              AS total_leads,
        COUNT(CASE WHEN d.is_closed_won = 1 THEN 1 END)    AS deals_won,
        ROUND(SUM(CASE WHEN d.is_closed_won = 1
                        THEN d.deal_value_usd ELSE 0 END), 0) AS revenue_won,
        ROUND(SUM(s.spend_usd) * 1.0
              / NULLIF(COUNT(CASE WHEN d.is_closed_won = 1 THEN 1 END), 0), 0)
                                                            AS cac,
        ROUND(SUM(CASE WHEN d.is_closed_won = 1
                        THEN d.deal_value_usd ELSE 0 END) * 1.0
              / NULLIF(SUM(s.spend_usd), 0), 2)            AS roas
    FROM marketing_spend s
    LEFT JOIN pipeline_deals d
        ON s.channel = d.lead_source_channel
        AND strftime('%Y-%m', s.date) = strftime('%Y-%m', d.created_date)
    GROUP BY s.channel
    ORDER BY roas DESC
"""

MONTHLY_TREND_SQL = """
    SELECT
        strftime('%Y-%m', s.date)                           AS month,
        ROUND(SUM(s.spend_usd), 0)                         AS spend,
        SUM(s.leads_generated)                              AS leads,
        COUNT(CASE WHEN d.is_closed_won = 1 THEN 1 END)    AS wins,
        ROUND(SUM(CASE WHEN d.is_closed_won = 1
                        THEN d.deal_value_usd ELSE 0 END), 0) AS revenue
    FROM marketing_spend s
    LEFT JOIN pipeline_deals d
        ON s.channel = d.lead_source_channel
        AND strftime('%Y-%m', s.date) = strftime('%Y-%m', d.created_date)
    GROUP BY month
    ORDER BY month
"""

SEGMENT_SQL = """
    SELECT
        d.segment,
        COUNT(*)                                            AS total_deals,
        COUNT(CASE WHEN d.is_closed_won = 1 THEN 1 END)    AS won,
        ROUND(AVG(d.deal_value_usd), 0)                    AS avg_deal_size,
        ROUND(COUNT(CASE WHEN d.is_closed_won = 1 THEN 1 END) * 100.0
              / COUNT(*), 1)                                AS win_rate_pct
    FROM pipeline_deals d
    GROUP BY d.segment
    ORDER BY avg_deal_size DESC
"""

TOP_CAMPAIGNS_SQL = """
    SELECT
        d.lead_source_campaign                              AS campaign,
        d.lead_source_channel                               AS channel,
        COUNT(CASE WHEN d.is_closed_won = 1 THEN 1 END)    AS deals_won,
        ROUND(SUM(CASE WHEN d.is_closed_won = 1
                        THEN d.deal_value_usd ELSE 0 END), 0) AS revenue
    FROM pipeline_deals d
    GROUP BY d.lead_source_campaign, d.lead_source_channel
    HAVING deals_won > 0
    ORDER BY revenue DESC
    LIMIT 10
"""


# ---------------------------------------------------------------------------
# 3. ANALYSIS & RECOMMENDATIONS
# ---------------------------------------------------------------------------

def generate_recommendations(channel_df):
    """Simple rule-based recommendations based on ROAS and CAC."""
    recs = []
    for _, row in channel_df.iterrows():
        ch = row["channel"]
        roas = row["roas"] if row["roas"] else 0
        cac = row["cac"] if row["cac"] else 0

        if roas >= 3.0:
            recs.append(f"  ✅  {ch:25s}  ROAS {roas:.1f}x — strong performer. Consider increasing budget 15-20%.")
        elif roas >= 1.5:
            recs.append(f"  🔄  {ch:25s}  ROAS {roas:.1f}x — solid but room to optimise targeting/creative.")
        elif roas > 0:
            recs.append(f"  ⚠️  {ch:25s}  ROAS {roas:.1f}x — underperforming. Review or reallocate spend.")
        else:
            recs.append(f"  ❓  {ch:25s}  No closed-won revenue yet — too early or attribution gap.")
    return recs


# ---------------------------------------------------------------------------
# 4. REPORTING
# ---------------------------------------------------------------------------

def print_section(title):
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")


def main():
    conn = load_data()

    # --- Channel Performance ---
    print_section("CHANNEL PERFORMANCE (H1 2025)")
    channel_df = pd.read_sql(CHANNEL_PERFORMANCE_SQL, conn)
    print(channel_df.to_string(index=False))

    # --- Monthly Trend ---
    print_section("MONTHLY PIPELINE TREND")
    monthly_df = pd.read_sql(MONTHLY_TREND_SQL, conn)
    print(monthly_df.to_string(index=False))

    # --- Segment Breakdown ---
    print_section("DEAL PERFORMANCE BY SEGMENT")
    segment_df = pd.read_sql(SEGMENT_SQL, conn)
    print(segment_df.to_string(index=False))

    # --- Top Campaigns ---
    print_section("TOP 10 CAMPAIGNS BY CLOSED-WON REVENUE")
    campaigns_df = pd.read_sql(TOP_CAMPAIGNS_SQL, conn)
    print(campaigns_df.to_string(index=False))

    # --- Recommendations ---
    print_section("BUDGET RECOMMENDATIONS FOR Q3 2025")
    for rec in generate_recommendations(channel_df):
        print(rec)

    # --- Summary Stats ---
    print_section("EXECUTIVE SUMMARY")
    total_spend = channel_df["total_spend"].sum()
    total_revenue = channel_df["revenue_won"].sum()
    total_deals = channel_df["deals_won"].sum()
    blended_roas = total_revenue / total_spend if total_spend else 0
    blended_cac = total_spend / total_deals if total_deals else 0

    print(f"  Total marketing spend (H1):   ${total_spend:>12,.0f}")
    print(f"  Total closed-won revenue:     ${total_revenue:>12,.0f}")
    print(f"  Total deals won:              {total_deals:>12,.0f}")
    print(f"  Blended ROAS:                 {blended_roas:>12.2f}x")
    print(f"  Blended CAC:                  ${blended_cac:>12,.0f}")

    conn.close()
    print(f"\n{'='*70}")
    print("  Analysis complete.")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    main()
