"""
visualization.py
----------------
📊 Builds interactive dashboards for overcharge analysis and refund insights.

Purpose:
--------
Provides visual summaries for internal analysts or clients using Plotly/Dash.
Shows trends, rate code patterns, and total refund distributions.

Workflow:
---------
1️⃣ Load validated error data.
2️⃣ Create key visualizations (bar, pie, trend charts).
3️⃣ Save as HTML dashboard.

Inputs:
-------
- Validated_Errors_Report.csv

Outputs:
--------
- Interactive HTML dashboard → /data/output/Refund_Insights.html

Depends On:
-----------
- pandas
- plotly.express
- src.utils.helpers
- src.utils.logger
"""

import pandas as pd
import plotly.express as px
from src.utils.helpers import load_csv, save_html
from src.utils.logger import get_logger

logger = get_logger(__name__)

def build_dashboard(file_name="Validated_Errors_Report.csv"):
    """
    Creates refund summary and trend visualizations.
    """
    try:
        df = load_csv("processed", file_name)
        if df.empty:
            logger.warning("⚠️ No data available for visualization.")
            return None

        fig1 = px.bar(df, x="rate_code", y="difference", color="issue_type",
                      title="Overcharge/Undercharge by Rate Code")

        fig2 = px.pie(df, names="validation_status", title="Validation Outcome Distribution")

        fig3 = px.scatter(df, x="usage_kwh", y="difference", color="rate_code",
                          title="Usage vs Overcharge Relationship")

        dashboard_path = "data/output/Refund_Insights.html"
        save_html([fig1, fig2, fig3], dashboard_path)

        logger.info(f"📊 Dashboard created: {dashboard_path}")
        return dashboard_path

    except Exception as e:
        logger.error(f"❌ Dashboard generation failed: {e}")
        return None
