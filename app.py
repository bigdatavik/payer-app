# import os
# from databricks import sql
# from databricks.sdk.core import Config
# import streamlit as st
# import pandas as pd


# # Ensure environment variable is set correctly
# assert os.getenv('DATABRICKS_WAREHOUSE_ID'), "DATABRICKS_WAREHOUSE_ID must be set in app.yaml."

# # Databricks config
# cfg = Config()

# # Query the SQL warehouse with Service Principal credentials
# def sql_query_with_service_principal(query: str) -> pd.DataFrame:
#     """Execute a SQL query and return the result as a pandas DataFrame."""
#     with sql.connect(
#         server_hostname=cfg.host,
#         http_path=f"/sql/1.0/warehouses/{cfg.warehouse_id}",
#         credentials_provider=lambda: cfg.authenticate  # Uses SP credentials from the environment variables
#     ) as connection:
#         with connection.cursor() as cursor:
#             cursor.execute(query)
#             return cursor.fetchall_arrow().to_pandas()

# # Query the SQL warehouse with the user credentials
# def sql_query_with_user_token(query: str, user_token: str) -> pd.DataFrame:
#     """Execute a SQL query and return the result as a pandas DataFrame."""
#     with sql.connect(
#         server_hostname=cfg.host,
#         http_path=f"/sql/1.0/warehouses/{cfg.warehouse_id}",
#         access_token=user_token  # Pass the user token into the SQL connect to query on behalf of user
#     ) as connection:
#         with connection.cursor() as cursor:
#             cursor.execute(query)
#             return cursor.fetchall_arrow().to_pandas()


import streamlit as st
import pandas as pd
from databricks import sql
from databricks.sdk.core import Config

# -----------------------------------------------------------------------------
# Streamlit & Page Setup
# -----------------------------------------------------------------------------
st.set_page_config(layout="wide")
st.title("💳 Claims Enriched Table Explorer (Horizontal Filters + Analytics)")

# -----------------------------------------------------------------------------
# Databricks SQL Connection Helper
# -----------------------------------------------------------------------------
cfg = Config()  # Uses your profile locally, or env vars in prod

@st.cache_resource
def get_connection():
    return sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{cfg.warehouse_id}",
        credentials_provider=lambda: cfg.authenticate
    )

def run_query(query: str) -> pd.DataFrame:
    conn = get_connection()
    with conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall_arrow().to_pandas()
    return result if result is not None else pd.DataFrame()

# -----------------------------------------------------------------------------
# 1. Selectors Row — CATALOG, SCHEMA, TABLE in one line
# -----------------------------------------------------------------------------
col1, col2, col3 = st.columns(3)

# --- Catalog ---
catalogs_df = run_query("SHOW CATALOGS")
if catalogs_df.empty:
    st.error("No catalogs available; check permissions.")
    st.stop()
catalog_col = catalogs_df.columns[0]
catalog_list = catalogs_df[catalog_col].dropna().tolist()
with col1:
    catalog_filter = st.text_input("🔎 Catalog filter", key="catalog_filter")
    filtered_catalogs = [c for c in catalog_list if catalog_filter.lower() in c.lower()]
    if not filtered_catalogs:
        st.warning("No catalogs match.")
        st.stop()
    selected_catalog = st.selectbox("Catalog", filtered_catalogs, key="catalog_select")

# --- Schema ---
selected_schema, schema_list = None, []
with col2:
    if selected_catalog:
        schemas_df = run_query(f"SHOW SCHEMAS IN {selected_catalog}")
        if schemas_df.empty:
            st.error("No schemas found.")
            st.stop()
        schema_col = schemas_df.columns[0]
        schema_list = schemas_df[schema_col].dropna().tolist()
        schema_filter = st.text_input("🔎 Schema filter", key="schema_filter")
        filtered_schemas = [s for s in schema_list if schema_filter.lower() in s.lower()]
        if not filtered_schemas:
            st.warning("No schemas match.")
            st.stop()
        selected_schema = st.selectbox("Schema", filtered_schemas, key="schema_select")

# --- Table ---
selected_table, table_list = None, []
with col3:
    if selected_catalog and selected_schema:
        tables_df = run_query(f"SHOW TABLES IN {selected_catalog}.{selected_schema}")
        if tables_df.empty:
            st.warning("No tables in this schema.")
            st.stop()
        tcol = "tableName" if "tableName" in tables_df.columns else tables_df.columns[1]
        table_list = tables_df[tcol].dropna().tolist()
        table_filter = st.text_input("🔎 Table filter", key="table_filter")
        filtered_tables = [t for t in table_list if table_filter.lower() in t.lower()]
        if not filtered_tables:
            st.warning("No tables match.")
            st.stop()
        default_idx = filtered_tables.index("claims_enriched") if "claims_enriched" in filtered_tables else 0
        selected_table = st.selectbox("Table", filtered_tables, index=default_idx, key="table_select")

# -----------------------------------------------------------------------------
# 2. Data Preview & Payor Analytics
# -----------------------------------------------------------------------------
if selected_catalog and selected_schema and selected_table:
    table_fqn = f"{selected_catalog}.{selected_schema}.{selected_table}"
    st.subheader(f"📊 Data from `{table_fqn}`")

    preview_df = run_query(f"SELECT * FROM {table_fqn} LIMIT 100")
    if not preview_df.empty:
        st.dataframe(preview_df, use_container_width=True)
    else:
        st.info("No data found.")

    # --- KPI Metrics Row ---
    kpi_query = f"""
        SELECT 
            COUNT(*) AS total_claims,
            SUM(COALESCE(total_charge, 0)) AS total_charges,
            COUNT(DISTINCT member_id) AS distinct_members,
            COUNT(DISTINCT provider_id) AS distinct_providers,
            SUM(CASE WHEN claim_status = 'denied' THEN 1 ELSE 0 END)/COUNT(*) AS denial_rate
        FROM {table_fqn}
    """
    kpi = run_query(kpi_query)
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Total Claims", int(kpi["total_claims"][0]))
    k2.metric("Total Charges", f"${kpi['total_charges'][0]:,.2f}")
    k3.metric("Unique Members", int(kpi["distinct_members"][0]))
    k4.metric("Unique Providers", int(kpi["distinct_providers"][0]))
    k5.metric("Denial Rate", f"{float(kpi['denial_rate'][0])*100:.1f}%")

    # --- Claims Status & Financial Trend ---
    c2a, c2b = st.columns(2)
    status_df = run_query(f"SELECT claim_status, COUNT(*) as n_claims FROM {table_fqn} GROUP BY claim_status")
    with c2a:
        st.markdown("#### Claims by Status")
        if not status_df.empty:
            st.bar_chart(status_df, x="claim_status", y="n_claims")
        else:
            st.info("No claims status data.")

    monthly_trend_q = f"""
        SELECT substr(claim_date,1,7) as month, SUM(total_charge) as charges,
               SUM(CASE WHEN claim_status='denied' THEN total_charge ELSE 0 END) as denied_amt
        FROM {table_fqn} GROUP BY month ORDER BY month
    """
    trend_df = run_query(monthly_trend_q)
    with c2b:
        st.markdown("#### Monthly Charges & Denials")
        if not trend_df.empty:
            st.line_chart(trend_df.set_index("month")[["charges", "denied_amt"]])
        else:
            st.info("No trend data.")

    # --- Denial Reasons & Denial Rate by Provider ---
    c3a, c3b = st.columns(2)
    # Denial reasons (by diagnosis_desc)
    denial_reason_q = f"""
        SELECT diagnosis_desc, COUNT(*) as denied_claims
        FROM {table_fqn}
        WHERE claim_status='denied'
        GROUP BY diagnosis_desc
        ORDER BY denied_claims DESC LIMIT 10
    """
    denial_reason_df = run_query(denial_reason_q)
    with c3a:
        st.markdown("#### Top Denial Reasons (Diagnosis)")
        if not denial_reason_df.empty:
            st.bar_chart(denial_reason_df, x="diagnosis_desc", y="denied_claims")
        else:
            st.info("No denials by reason.")

    # Denial rate by provider
    denial_by_provider_q = f"""
        SELECT provider_name,
               SUM(CASE WHEN claim_status='denied' THEN 1 ELSE 0 END)/COUNT(*) as denial_rate,
               COUNT(*) as total
        FROM {table_fqn}
        GROUP BY provider_name HAVING total >= 3
        ORDER BY denial_rate DESC LIMIT 10
    """
    denial_by_provider = run_query(denial_by_provider_q)
    with c3b:
        st.markdown("#### Providers with Highest Denial Rate")
        if not denial_by_provider.empty:
            st.bar_chart(denial_by_provider.set_index("provider_name")["denial_rate"])
        else:
            st.info("No denial/provider data.")

    # --- Diagnoses analytics & provider leaderboard ---
    c4a, c4b = st.columns(2)
    diag_q = f"""
        SELECT diagnosis_desc, COUNT(*) as n_claims, SUM(total_charge) as charges
        FROM {table_fqn}
        GROUP BY diagnosis_desc ORDER BY charges DESC LIMIT 10
    """
    diag_df = run_query(diag_q)
    with c4a:
        st.markdown("#### Top Diagnoses by Cost")
        if not diag_df.empty:
            st.bar_chart(diag_df, x="diagnosis_desc", y="charges")
        else:
            st.info("No diagnoses data.")

    prov_q = f"""
        SELECT provider_name, SUM(total_charge) as charges, COUNT(*) as n_claims
        FROM {table_fqn}
        GROUP BY provider_name ORDER BY charges DESC LIMIT 10
    """
    prov_df = run_query(prov_q)
    with c4b:
        st.markdown("#### Top Providers by Total Charge")
        if not prov_df.empty:
            st.dataframe(prov_df)
        else:
            st.info("No provider data.")

    # --- Outlier Claims: High Charge ---
    st.markdown("#### Outlier High-Charge Claims")
    outlier_q = f"""
        SELECT *
        FROM {table_fqn}
        WHERE total_charge > (
            SELECT AVG(total_charge) + 3*STDDEV(total_charge) FROM {table_fqn}
        )
        ORDER BY total_charge DESC LIMIT 10
    """
    outliers_df = run_query(outlier_q)
    if not outliers_df.empty:
        st.dataframe(outliers_df)
    else:
        st.info("No outlier claims found.")
