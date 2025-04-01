import pyodbc
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
import re

# Retrieve credentials from Streamlit secrets
sql_server = st.secrets["sql"]["SQL_SERVER"]
sql_database_1 = st.secrets["sql"]["SQL_DATABASE_1"]
sql_uid = st.secrets["sql"]["SQL_UID"]
sql_pass = st.secrets["sql"]["SQL_PASS"]
sql_driver = "ODBC Driver 17 for SQL Server"  # Update as necessary

# Define the function to establish a DB connection
def establish_db_connection():
    """Establish a connection to SQL Server using pyodbc."""
    try:
        conn_str = (
            f"DRIVER={{{sql_driver}}};"
            f"SERVER={sql_server};"
            f"DATABASE={sql_database_1};"
            f"UID={sql_uid};"
            f"PWD={sql_pass};"
        )
        conn = pyodbc.connect(conn_str)
        print("PyODBC connection established!")
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None

@st.cache_data
def load_data_from_sql(query):
    """Load data from SQL Server into a pandas DataFrame using pyodbc."""
    conn = establish_db_connection()
    if conn is None:
        st.error("Failed to establish database connection.")
        return None
    
    try:
        df = pd.read_sql(query, conn)
        conn.close()
        
        if df.empty:
            st.warning("No data returned from the query.")
        else:
            st.success("Data loaded successfully.")
        return df
    except Exception as e:
        st.error(f"Error loading data: {e}")
        print(f"Error: {e}")  # For debugging in the terminal
        return None
    
# SQL Query to extract subscription data
sql_query = """
	------------------------------------------------------------------------------------------------
WITH SubscriptionData AS (
    SELECT 
        deal.Deal_id,
        deal.deal_pipeline_id,
        company.id AS Company_ID, 
        company.property_name,
        deal.deal_pipeline_stage_id,
        aggregated.property_bundle,
        aggregated.property_product_category, -- Track Product Category for Renewal Logic
        MIN(line_item.property_subscription_start_date) AS MIN_Subscription_Start_Date,
        MAX(line_item.property_subscription_end_date) AS MAX_Subscription_End_Date,
        aggregated.Total_Amount,

        -- Subscription Status: LIVE or EXPIRED
        CASE 
            WHEN MAX(line_item.property_subscription_end_date) >= GETDATE() THEN 'LIVE' 
            ELSE 'EXPIRED' 
        END AS Subscription_Status,

        -- Begin Period (Start Date)
        FORMAT(MIN(line_item.property_subscription_start_date), 'yyyy-MM-dd') AS Begin_Period_Date,

        -- Renewal Period (End Date)
        FORMAT(MAX(line_item.property_subscription_end_date), 'yyyy-MM-dd') AS Renewal_Period_Date,

        -- ACV Calculation (Annual Contract Value)
        ROUND(
            (aggregated.Total_Amount / 
            NULLIF(DATEDIFF(DAY, MIN(line_item.property_subscription_start_date), MAX(line_item.property_subscription_end_date)) + 1, 0))
            * 365, 2
        ) AS ACV

    FROM 
        [_hubspot].[deal] AS deal
    LEFT JOIN 
        (
            -- Subquery to aggregate total amount
            SELECT 
                line_item_deal.deal_id,
                product.property_bundle,
                product.property_product_category,
                SUM(line_item.property_amount) AS Total_Amount
            FROM 
                [_hubspot].[line_item_deal] AS line_item_deal
            INNER JOIN 
                [_hubspot].[line_item] AS line_item
                ON line_item_deal.line_item_id = line_item.id
            INNER JOIN 
                [_hubspot].[product] AS product
                ON line_item.product_id = product.id
            GROUP BY 
                line_item_deal.deal_id,
                product.property_bundle,
                product.property_product_category
        ) AS aggregated
        ON deal.Deal_id = aggregated.deal_id
    LEFT JOIN 
        [_hubspot].[deal_pipeline_stage] AS pipeline_labels
        ON pipeline_labels.stage_id = deal.deal_pipeline_stage_id
    LEFT JOIN 
        [_hubspot].[deal_company] AS deal_company
        ON deal_company.deal_id = deal.deal_id
    LEFT JOIN 
        [_hubspot].[company] AS company
        ON company.id = deal_company.company_id
    LEFT JOIN 
        [_hubspot].[line_item_deal] AS line_item_deal
        ON deal.Deal_id = line_item_deal.deal_id
    LEFT JOIN 
        [_hubspot].[line_item] AS line_item
        ON line_item_deal.line_item_id = line_item.id
    WHERE 
        pipeline_labels.label IN ('Closed won', 'Closed Won Approved', 'Renewal due', 'Cancelled Subscription') 
        AND deal.deal_pipeline_id IN ('default', '1305376', '1313057', '2453638', '6617404', '17494655', '1305377')  
    GROUP BY 
        deal.Deal_id,
        deal.deal_pipeline_id,
        company.id,
        company.property_name,
        deal.deal_pipeline_stage_id,
        aggregated.property_bundle,
        aggregated.property_product_category,
        aggregated.Total_Amount
),
RenewalCheck AS (
    SELECT 
        sd1.*,

        -- Check if the same Company and Product Category have a new deal starting after expiration
        CASE 
            WHEN EXISTS (
                SELECT 1 
                FROM SubscriptionData sd2 
                WHERE sd1.Company_ID = sd2.Company_ID 
                AND sd1.property_product_category = sd2.property_product_category
                AND YEAR(sd1.MAX_Subscription_End_Date) = YEAR(sd2.MIN_Subscription_Start_Date)
            ) 
            THEN 'Renewed'
            ELSE 'Not Renewed'
        END AS Renewal_Status

    FROM SubscriptionData sd1
),
FinalStatus AS (
    SELECT 
        rc.*,

        -- Adjusted Renewal Status Logic
        CASE 
            WHEN rc.Subscription_Status = 'LIVE' AND rc.Renewal_Status = 'Not Renewed' THEN 'Due for Renewal'
            WHEN rc.Subscription_Status = 'EXPIRED' AND rc.Renewal_Status = 'Not Renewed' THEN 'Non Renewal'
            ELSE rc.Renewal_Status
        END AS Final_Renewal_Status

    FROM RenewalCheck rc
)
SELECT * FROM FinalStatus
ORDER BY property_name, MAX_Subscription_End_Date DESC;
----------------------------------------------------------------

"""


# Function to extract year from Renewal_Period_Date
def extract_year(value):
    match = re.search(r'\b(20\d{2})\b', str(value))  # Look for '20XX' format
    return int(match.group(1)) if match else None  # Return year or None

# Load data from SQL
df = load_data_from_sql(sql_query)

# Convert date columns
df['MIN_Subscription_Start_Date'] = pd.to_datetime(df['MIN_Subscription_Start_Date'], errors='coerce').dt.date
df['MAX_Subscription_End_Date'] = pd.to_datetime(df['MAX_Subscription_End_Date'], errors='coerce').dt.date
df['Renewal_Period_Date'] = pd.to_datetime(df['Renewal_Period_Date'], errors='coerce')

# Extract Year and Month from Renewal_Period_Date
df['Renewal_Month'] = df['Renewal_Period_Date'].dt.month.fillna(0).astype(int)
df['Renewal_Year'] = df['Renewal_Period_Date'].dt.year.fillna(0).astype(int)

# Sidebar Filters
st.sidebar.header("Filters")
selected_category = st.sidebar.selectbox("Select Product Category", ["All"] + list(df['property_product_category'].dropna().unique()))
selected_bundle = st.sidebar.selectbox("Select Product Bundle", ["All"] + list(df['property_bundle'].dropna().unique()))
selected_year = st.sidebar.selectbox("Select Year", range(2020, 2031), index=5)  # Adjust range as needed

# ACV Calculations for Selected Year
start_date = pd.to_datetime(f"{selected_year}-01-01").date()
end_date = pd.to_datetime(f"{selected_year}-12-31").date()

filtered_df = df.copy()
if selected_category != "All":
    filtered_df = filtered_df[filtered_df['property_product_category'] == selected_category]
if selected_bundle != "All":
    filtered_df = filtered_df[filtered_df['property_bundle'] == selected_bundle]

# ACV Calculations
opening_acv = filtered_df[(filtered_df['MIN_Subscription_Start_Date'] <= start_date) & 
                          (filtered_df['MAX_Subscription_End_Date'] >= start_date)]['ACV'].sum()

expiring_acv = filtered_df[(filtered_df['MAX_Subscription_End_Date'] >= start_date) & 
                           (filtered_df['MAX_Subscription_End_Date'] <= end_date)]['ACV'].sum()

renewed_acv = filtered_df[(filtered_df['Final_Renewal_Status'] == "Renewed") & 
                          (filtered_df['Renewal_Year'] == selected_year)]['ACV'].sum()

new_business_acv = filtered_df[(filtered_df['MIN_Subscription_Start_Date'] >= start_date) & 
                               (filtered_df['MIN_Subscription_Start_Date'] <= end_date) & 
                               (filtered_df['deal_pipeline_id'] == "default")]['ACV'].sum()

closing_acv = opening_acv + renewed_acv + new_business_acv - expiring_acv

# Waterfall Chart for ACV Breakdown
waterfall_data = [
    {"x": "Opening ACV", "y": opening_acv},
    {"x": "Expiring ACV", "y": -expiring_acv},
    {"x": "Renewed ACV", "y": renewed_acv},
    {"x": "New Business ACV", "y": new_business_acv},
    {"x": "Closing ACV", "y": closing_acv}
]

fig_acv = go.Figure(go.Waterfall(
    x=[item["x"] for item in waterfall_data],
    y=[item["y"] for item in waterfall_data],
    text=[f"£{abs(value):,.2f}" for value in [item["y"] for item in waterfall_data]],
    decreasing={"marker": {"color": "red"}},
    increasing={"marker": {"color": "green"}}
))

fig_acv.update_layout(
    title="Waterfall Chart for ACV",
    xaxis_title="ACV Breakdown",
    yaxis_title="Amount (£)",
    showlegend=False
)

st.plotly_chart(fig_acv)

# Display ACV Values
st.write(f"### ACV Breakdown for {selected_year}")
st.write(f"**Opening ACV:** £{opening_acv:,.2f}")
st.write(f"**Expiring ACV:** £{expiring_acv:,.2f}")
st.write(f"**Renewed ACV:** £{renewed_acv:,.2f}")
st.write(f"**New Business ACV:** £{new_business_acv:,.2f}")
st.write(f"**Closing ACV:** £{closing_acv:,.2f}")



# User selects year and month
selected_month = st.sidebar.selectbox("Select Month", range(1, 13), format_func=lambda x: pd.to_datetime(f"{selected_year}-{x:02d}-01").strftime('%B'))

# Define time period for the selected month
month_start = pd.to_datetime(f"{selected_year}-{selected_month:02d}-01").date()
month_end = (pd.to_datetime(month_start) + pd.offsets.MonthEnd(0)).date()

# Calculate Opening ACV for the selected month
opening_acv = filtered_df[
    (filtered_df['MIN_Subscription_Start_Date'] <= month_start) &  # Started before or at the month
    (filtered_df['MAX_Subscription_End_Date'] >= month_start)  # Still active at the start of the month
]['ACV'].sum()

# Calculate Expiring ACV for the selected month
expiring_acv = filtered_df[
    (filtered_df['MAX_Subscription_End_Date'] >= month_start) & 
    (filtered_df['MAX_Subscription_End_Date'] <= month_end)  # Ends within the month
]['ACV'].sum()

# Calculate Renewed ACV for the selected month
renewed_acv = filtered_df[
    (filtered_df['Final_Renewal_Status'] == "Renewed") &
    (filtered_df['Renewal_Month'] == selected_month)  # Renewed in the selected month
]['ACV'].sum()

# Calculate New Business ACV for the selected month
new_business_acv = filtered_df[
    (filtered_df['MIN_Subscription_Start_Date'] >= month_start) & 
    (filtered_df['MIN_Subscription_Start_Date'] <= month_end) & 
    (filtered_df['deal_pipeline_id'] == "default")  # Only default pipeline deals
]['ACV'].sum()

# Calculate Closing ACV
closing_acv = opening_acv - expiring_acv + renewed_acv + new_business_acv

# Waterfall Chart Data
waterfall_values = [opening_acv, -expiring_acv, renewed_acv, new_business_acv, closing_acv]
waterfall_labels = ["Opening ACV", "Expiring", "Renewed", "New Business", "Closing ACV"]
waterfall_colors = ["blue", "red", "green", "green", "blue"]  # Color coding

# Create Waterfall Chart
fig = go.Figure(go.Waterfall(
    x=waterfall_labels,
    y=waterfall_values,
    text=[f"£{value:,.2f}" for value in waterfall_values],
    decreasing={"marker": {"color": "red"}},
    increasing={"marker": {"color": "green"}},
    connector={"line": {"color": "gray"}}
))

fig.update_layout(
    title=f"ACV Breakdown for {pd.to_datetime(month_start).strftime('%B %Y')}",
    yaxis_title="ACV (£)",
    showlegend=False
)

# Display the chart
st.plotly_chart(fig)

# Display ACV Values
st.write(f"### ACV Breakdown for {pd.to_datetime(month_start).strftime('%B %Y')}")
st.write(f"**Opening ACV:** £{opening_acv:,.2f}")
st.write(f"**Expiring ACV:** £{expiring_acv:,.2f}")
st.write(f"**Renewed ACV:** £{renewed_acv:,.2f}")
st.write(f"**New Business ACV:** £{new_business_acv:,.2f}")
st.write(f"**Closing ACV:** £{closing_acv:,.2f}")
