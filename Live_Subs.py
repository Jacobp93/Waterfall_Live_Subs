from sqlalchemy import create_engine
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
import re
import datetime
import calendar
from datetime import timedelta


# Retrieve credentials from Streamlit secrets
sql_server = st.secrets["sql"]["SQL_SERVER"]
sql_database_1 = st.secrets["sql"]["SQL_DATABASE_1"]
sql_uid = st.secrets["sql"]["SQL_UID"]
sql_pass = st.secrets["sql"]["SQL_PASS"]
sql_driver = "ODBC Driver 17 for SQL Server" 



# Define the function to establish a DB connection
def establish_db_connection():
    """Establish a connection to SQL Server using SQLAlchemy."""
    try:
        # Constructing the connection string for SQLAlchemy
        conn_str = f"mssql+pyodbc://{sql_uid}:{sql_pass}@{sql_server}/{sql_database_1}?driver={sql_driver}"
        
        # Creating the engine with the connection string
        engine = create_engine(conn_str)
        
        # Check if the connection is successful
        print("SQLAlchemy connection established!")
        return engine
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None

@st.cache_data
def load_data_from_sql(query):
    """Load data from SQL Server into a pandas DataFrame using SQLAlchemy."""
    engine = establish_db_connection()
    if engine is None:
        st.error("Failed to establish database connection.")
        return None
    
    try:
        # Use the SQLAlchemy engine to load the data into a pandas DataFrame
        df = pd.read_sql(query, engine)
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
        company.property_region_dfe_,
        deal.deal_pipeline_stage_id,
        aggregated.property_bundle,
        aggregated.property_product_category,
        MIN(line_item.property_subscription_start_date) AS MIN_Subscription_Start_Date,
        MAX(line_item.property_subscription_end_date) AS MAX_Subscription_End_Date,
        aggregated.Total_Amount,

        -- Subscription Status
        CASE 
            WHEN MAX(line_item.property_subscription_end_date) >= GETDATE() THEN 'LIVE' 
            ELSE 'EXPIRED' 
        END AS Subscription_Status,

        -- Begin Period (Start Date)
        FORMAT(MIN(line_item.property_subscription_start_date), 'yyyy-MM-dd') AS Begin_Period_Date,

        -- Renewal Period (End Date + 1 day, formatted to month)
        FORMAT(DATEADD(DAY, 1, MAX(line_item.property_subscription_end_date)), 'yyyy-MM') AS Renewal_Period,

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
            -- Aggregate total amount and calculate min/max dates per product
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
        company.property_region_dfe_,
        deal.deal_pipeline_stage_id,
        aggregated.property_bundle,
        aggregated.property_product_category,
        aggregated.Total_Amount
),
RenewalCheck AS (
    SELECT 
        sd1.*,

        -- Renewal logic with 12-month future window and backdating to original start
        CASE 
            WHEN sd1.Subscription_Status = 'LIVE' THEN 'Due for Renewal'
            WHEN EXISTS (
                SELECT 1 
                FROM SubscriptionData sd2 
                WHERE 
                    sd1.Company_ID = sd2.Company_ID 
                    AND sd1.property_product_category = sd2.property_product_category
                    AND sd2.Deal_id <> sd1.Deal_id
                    AND sd2.MIN_Subscription_Start_Date BETWEEN sd1.MIN_Subscription_Start_Date AND DATEADD(YEAR, 1, sd1.MAX_Subscription_End_Date)
            ) THEN 'Renewed'
            ELSE 'Not Renewed'
        END AS Renewal_Status

    FROM SubscriptionData sd1
),
FinalStatus AS (
    SELECT 
        rc.*,
        CASE 
            WHEN rc.Subscription_Status = 'LIVE' AND rc.Renewal_Status = 'Not Renewed' THEN 'Due for Renewal'
            WHEN rc.Subscription_Status = 'EXPIRED' AND rc.Renewal_Status = 'Not Renewed' THEN 'Non Renewal'
            ELSE rc.Renewal_Status
        END AS Final_Renewal_Status
    FROM RenewalCheck rc
)
SELECT * 
FROM FinalStatus
ORDER BY property_name, MAX_Subscription_End_Date DESC;

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
df['Renewal_Period'] = pd.to_datetime(df['Renewal_Period'], errors='coerce')

# Extract Year and Month from Renewal_Period_Date
df['Renewal_Month'] = df['Renewal_Period'].dt.month.fillna(0).astype(int)
df['Renewal_Year'] = df['Renewal_Period'].dt.year.fillna(0).astype(int)

# Sidebar Filters
st.sidebar.header("Filters")
selected_region = st.sidebar.selectbox("Select Region",["All"] +list(df['property_region_dfe_'].dropna().unique()))
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
if selected_region != "All":
    filtered_df = filtered_df[filtered_df['property_region_dfe_'] == selected_region]

# ACV Calculations
opening_acv = filtered_df[(filtered_df['MIN_Subscription_Start_Date'] <= start_date) & 
                          (filtered_df['MAX_Subscription_End_Date'] >= start_date)]['ACV'].sum()

 # Expiring ACV (Anything expiring in the current month)
expiring_acv = filtered_df[
            (filtered_df['Renewal_Year'] == selected_year)
        ]['ACV'].sum()

renewed_acv = filtered_df[(filtered_df['Final_Renewal_Status'] == "Renewed") & 
                          (filtered_df['Renewal_Year'] == selected_year)]['ACV'].sum()

new_business_acv = filtered_df[(filtered_df['MIN_Subscription_Start_Date'] >= start_date) & 
                               (filtered_df['MIN_Subscription_Start_Date'] <= end_date) & 
                               (filtered_df['deal_pipeline_id'] == "default")]['ACV'].sum()

closing_acv = opening_acv - expiring_acv + renewed_acv + new_business_acv

# Title with Selected Year
st.markdown(f"<h2 style='text-align: center;'>ACV Breakdown for {selected_year}</h2>", unsafe_allow_html=True)

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


# Sample DataFrame (replace this with your actual DataFrame)
# Assuming 'filtered_df' contains the necessary data with proper columns such as 'ACV', 'Subscription Start Date', 'Subscription End Date', 'Renewal_Year', 'Renewal_Month', etc.

# Define month names for selection
month_map = {i: pd.to_datetime(f"2024-{i:02d}-01").strftime('%B') for i in range(1, 13)}

start_month = st.sidebar.selectbox("Select Start Month", list(month_map.keys()), index=0, key="start_month_selector")
end_month = st.sidebar.selectbox("Select End Month", list(month_map.keys()), index=11, key="end_month_selector")

# Ensure start month is before end month
if start_month > end_month:
    st.sidebar.error("Start month should be before end month.")
else:
    st.markdown(f"<h2 style='text-align: center;'>ACV Breakdown for {month_map[start_month]} to {month_map[end_month]} {selected_year}</h2>", unsafe_allow_html=True)

    # Initialize variables to accumulate totals
    opening_acv = 0
    expiring_acv = 0
    renewed_acv = 0
    new_business_acv = 0
    closing_acv = 0
    
    # Calculate Opening ACV at the start of the first selected months
    first_month_start = pd.to_datetime(f"{selected_year}-{start_month:02d}-01").date()
    opening_acv = filtered_df[
        (filtered_df['MIN_Subscription_Start_Date'] <= first_month_start) & 
        (filtered_df['MAX_Subscription_End_Date'] >= first_month_start)
    ]['ACV'].sum()

    # Init rolling values for cumulative ACV changes
    rolling_acv = opening_acv

    # Loop through the selected months and accumulate
    for month in range(start_month, end_month + 1):
        month_start = pd.to_datetime(f"{selected_year}-{month:02d}-01").date()
        month_end = (pd.to_datetime(month_start) + pd.offsets.MonthEnd(0)).date()

        # Expiring ACV (Anything expiring in the current month)
        expiring = filtered_df[
            (filtered_df['Renewal_Year'] == selected_year) &
            (filtered_df['Renewal_Month'] == month)
        ]['ACV'].sum()

        # Renewed ACV
        renewed = filtered_df[
            (filtered_df['Final_Renewal_Status'] == "Renewed") &
            (filtered_df['Renewal_Year'] == selected_year) &
            (filtered_df['Renewal_Month'] == month)
        ]['ACV'].sum()

        # New Business ACV (New subscriptions started in the current month)
        new_business = filtered_df[
            (filtered_df['MIN_Subscription_Start_Date'] >= month_start) &
            (filtered_df['MIN_Subscription_Start_Date'] <= month_end) &
            (filtered_df['deal_pipeline_id'] == "default")  
        ]['ACV'].sum()

        closing_acv = opening_acv - expiring + new_business_acv



        # Accumulate values
        expiring_acv += expiring
        renewed_acv += renewed
        new_business_acv += new_business
        
        # Update the rolling ACV (Closing ACV for the month)
        rolling_acv = rolling_acv - expiring + renewed + new_business

    # Prepare values for the chart (5 bars)
    labels = ["Opening ACV", "Expiring ACV", "Renewed ACV", "New Business ACV", "Closing ACV"]
    values = [opening_acv, -expiring_acv, renewed_acv, new_business_acv, closing_acv]
    
    # Create the Waterfall chart
    fig = go.Figure(go.Waterfall(
        x=labels,
        y=values,
        text=[f"£{v:,.2f}" for v in values],
        decreasing={"marker": {"color": "red"}},
        increasing={"marker": {"color": "green"}},
        connector={"line": {"color": "gray"}}
    ))

    fig.update_layout(
        title=f"ACV Waterfall: {month_map[start_month]} to {month_map[end_month]} {selected_year}",
        yaxis_title="ACV (£)",
        showlegend=False
    )

    
    st.plotly_chart(fig)



    # Third Chart: Opening ACV, Expiring ACV, and Closing ACV
    st.markdown(f"<h3 style='text-align: center;'>Simplified ACV View: {month_map[start_month]} to {month_map[end_month]} {selected_year}</h3>", unsafe_allow_html=True)

    # Data for the simplified chart
    simple_labels = ["Opening ACV", "Expiring ACV", "New Business" , "Closing ACV"]
    simple_values = [opening_acv, -expiring_acv,new_business_acv , closing_acv]

    fig_simple = go.Figure(go.Waterfall(
        x=simple_labels,
        y=simple_values,
        text=[f"£{v:,.2f}" for v in simple_values],
        decreasing={"marker": {"color": "red"}},
        increasing={"marker": {"color": "green"}},
        connector={"line": {"color": "gray"}}
    ))

    fig_simple.update_layout(
        title="Simplified ACV Waterfall: Opening → Expiring → Closing",
        yaxis_title="ACV (£)",
        showlegend=False
    )

    st.plotly_chart(fig_simple)
