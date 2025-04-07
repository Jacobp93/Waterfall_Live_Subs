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
sql_driver = "ODBC Driver 17 for SQL Server"  # Update as necessary




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
FORMAT(
    DATEADD(DAY, 1, MAX(line_item.property_subscription_end_date)), 
    'yyyy-MM'
) AS Renewal_Period_Date,

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
		company.property_region_dfe_,
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

expiring_acv = filtered_df[(filtered_df['MAX_Subscription_End_Date'] >= start_date) & 
                           (filtered_df['MAX_Subscription_End_Date'] <= end_date)]['ACV'].sum()

renewed_acv = filtered_df[(filtered_df['Final_Renewal_Status'] == "Renewed") & 
                          (filtered_df['Renewal_Year'] == selected_year)]['ACV'].sum()

new_business_acv = filtered_df[(filtered_df['MIN_Subscription_Start_Date'] >= start_date) & 
                               (filtered_df['MIN_Subscription_Start_Date'] <= end_date) & 
                               (filtered_df['deal_pipeline_id'] == "default")]['ACV'].sum()

closing_acv = opening_acv + renewed_acv + new_business_acv - expiring_acv

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


# Sidebar date range selection
start_date, end_date = st.sidebar.date_input(
    "Select Date Range", 
    value=(pd.to_datetime("2024-01-01").date(), pd.to_datetime("2024-12-31").date())
)

# Convert to year and month for calculations
start_year, start_month = start_date.year, start_date.month
end_year, end_month = end_date.year, end_date.month

st.markdown(f"<h2 style='text-align: center;'>ACV Breakdown for {calendar.month_name[start_month]} {start_year} to {calendar.month_name[end_month]} {end_year}</h2>", unsafe_allow_html=True)

# Calculate Opening ACV at the start of the first selected month
opening_acv = filtered_df[
    (filtered_df['MIN_Subscription_Start_Date'] <= start_date) &
    (filtered_df['MAX_Subscription_End_Date'] >= start_date)
]['ACV'].sum()

# Initialize rolling values
rolling_acv = opening_acv
cumulative_expiring_acv = 0
cumulative_renewed_acv = 0
cumulative_new_business_acv = 0

# Define labels and values for the waterfall chart
labels = ["Opening ACV"]
values = [opening_acv]

# Loop through each month in the selected date range
current_year, current_month = start_year, start_month
while (current_year < end_year) or (current_year == end_year and current_month <= end_month):
    # Set the start and end dates for the current month
    month_start = pd.to_datetime(f"{current_year}-{current_month:02d}-01").date()
    month_end = (pd.to_datetime(month_start) + pd.offsets.MonthEnd(0)).date()

    # Add one day to month_end for calculating expiring subscriptions correctly
    next_month_end = month_end + timedelta(days=1)

    # Expiring ACV for the current month (subscriptions ending between month_end and next_month_end)
    expiring_acv = filtered_df[
        (filtered_df['Renewal_Year'] == current_year) &
        (filtered_df['Renewal_Month'] == current_month)
    ]['ACV'].sum()

    # Renewed ACV for the current month
    renewed_acv = filtered_df[
        (filtered_df['Final_Renewal_Status'] == "Renewed") &
        (filtered_df['Renewal_Year'] == current_year) &
        (filtered_df['Renewal_Month'] == current_month)
    ]['ACV'].sum()

    # New Business ACV for the current month (new subscriptions starting within the current month)
    new_business_acv = filtered_df[
        (filtered_df['MIN_Subscription_Start_Date'] >= month_start) &
        (filtered_df['MIN_Subscription_Start_Date'] <= month_end) &
        (filtered_df['deal_pipeline_id'] == "default")
    ]['ACV'].sum()

    # Update cumulative values
    cumulative_expiring_acv += expiring_acv
    cumulative_renewed_acv += renewed_acv
    cumulative_new_business_acv += new_business_acv

    # Update rolling ACV: Add Renewed and New Business ACV, subtract Expiring ACV
    rolling_acv = rolling_acv - expiring_acv + renewed_acv + new_business_acv

    # Append each month’s details to the chart series
    values.extend([-expiring_acv, renewed_acv, new_business_acv])
    labels.extend([
        f"{calendar.month_abbr[current_month]} Expiring",
        f"{calendar.month_abbr[current_month]} Renewed",
        f"{calendar.month_abbr[current_month]} New"
    ])

    # Move to the next month
    if current_month == 12:
        current_month = 1
        current_year += 1
    else:
        current_month += 1

# Append closing ACV
labels.append("Closing ACV")
values.append(rolling_acv)

# Create the Waterfall Chart
fig = go.Figure(go.Waterfall(
    x=labels,
    y=values,
    text=[f"£{v:,.2f}" for v in values],
    decreasing={"marker": {"color": "red"}},
    increasing={"marker": {"color": "green"}},
    connector={"line": {"color": "gray"}}
))

fig.update_layout(
    title=f"ACV Waterfall: {calendar.month_name[start_month]} {start_year} to {calendar.month_name[end_month]} {end_year}",
    yaxis_title="ACV (£)",
    showlegend=False
)

# Show the chart
st.plotly_chart(fig)

# Display the ACV metrics
col1, col2, col3, col4, col5 = st.columns([1.5, 1.5, 1.5, 1.5, 1.5])

with col1:
    st.markdown("<p style='font-size:14px; text-align:center;'>Opening ACV</p>", unsafe_allow_html=True)
    st.write(f"**£{opening_acv:,.2f}**")

with col2:
    st.markdown("<p style='font-size:14px; text-align:center;'>Expiring ACV</p>", unsafe_allow_html=True)
    st.write(f"**£{cumulative_expiring_acv:,.2f}**")

with col3:
    st.markdown("<p style='font-size:14px; text-align:center;'>Renewed ACV</p>", unsafe_allow_html=True)
    st.write(f"**£{cumulative_renewed_acv:,.2f}**")

with col4:
    st.markdown("<p style='font-size:14px; text-align:center;'>New Business ACV</p>", unsafe_allow_html=True)
    st.write(f"**£{cumulative_new_business_acv:,.2f}**")

with col5:
    st.markdown("<p style='font-size:14px; text-align:center;'>Closing ACV</p>", unsafe_allow_html=True)
    st.write(f"**£{rolling_acv:,.2f}**")
