import streamlit as st
import folium
from streamlit_folium import st_folium
import mysql.connector
import pandas as pd



def connect_database():
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Philo@leo92",
        database="PhonePe"
    )
    return mydb


mydb = connect_database()
query = mydb.cursor()

states_option = ["andaman-&-nicobar-islands", "andhra-pradesh", "arunachal-pradesh", "assam", "bihar", "chandigarh",
 "chhattisgarh", "dadra-&-nagar-haveli-&-daman-&-diu", "delhi", "goa", "gujarat", "haryana",
 "himachal-pradesh", "jammu-&-kashmir", "jharkhand", "karnataka", "kerala", "ladakh", "lakshadweep",
 "madhya-pradesh", "maharashtra", "manipur", "meghalaya", "mizoram", "nagaland", "odisha", "puducherry",
 "punjab", "rajasthan", "sikkim", "tamil-nadu", "telangana", "tripura", "uttar-pradesh", "uttarakhand",
 "west-bengal"]
# dadra-&-nagar-haveli-&-daman-&-diu, ladakh
year_option = [2018, 2019, 2020, 2021, 2022]
quarter = [1, 2, 3, 4]


st.set_page_config(page_icon="Projects/Phonepe_Pulse/PhonePe_Logo.png", page_title="PhonePe Pulse", layout="wide")
st.title(":violet[PhonePe Pulse | THE BEAT OF PROGRESS]")

col1t, col2t = st.columns(2)
with col1t:
    Year = st.selectbox("**:green[Select Year: ]**", year_option)
with col2t:
    Quarter = st.selectbox("**:green[Select Quarter: ]**", quarter)

tab1, tab2 = st.tabs(["Transaction", "Users"])

with tab1:
    st.subheader(f"**:violet[INDIA Transaction Details- Q{Quarter}, {Year}]**")
    col1i, col2i = st.columns(2)
    with col1i:
        qry1 = f"select sum(Transaction_Count) as Total_Transactions, sum(Transaction_Amount) as Total_Transaction_Amount" \
               f" from agg_trans where Year={Year} and Quarter={Quarter}"
        IndiaTotal = pd.read_sql_query(qry1, mydb)
        st.write(f"**:orange[_All PhonePe Transactions(UPI+Cards+Wallets) :_] "
                 f":black[{round(IndiaTotal.iloc[0, 0])}]**")
        st.write(f"**:orange[_Total Payment Value :_] :black[Rs. {round(IndiaTotal.iloc[0, 1])}]**")
        Average_Transaction_Value = (IndiaTotal.iloc[0, 1]/IndiaTotal.iloc[0, 0])
        st.write(f"**:orange[_Average Transaction Value :_] :black[Rs. {round(Average_Transaction_Value)}]**")

    with col2i:
        st.write("**:orange[_Categories :_]**")
        qry2 = f"select Transaction_Type, sum(Transaction_Count) as Transaction_Count, " \
               f"sum(Transaction_Amount) as Transaction_Amount from agg_trans " \
              f"where Year={Year} and Quarter={Quarter} group by Transaction_Type"
        IndiaCategory = pd.read_sql_query(qry2, mydb)
        st.dataframe(IndiaCategory)

    State = st.selectbox("**:green[Select State: ]**", states_option)
    qry = f"select Transaction_Type, Transaction_Count, Transaction_Amount from agg_trans" \
          f" where State='{State}' and Year={Year} and Quarter={Quarter}"
    df = pd.read_sql_query(qry, mydb)
    st.write(f"**:orange[_{State.upper()} Transaction Details_]**")
    st.dataframe(df)

    st.subheader(f":violet[Map Details - Q{Quarter}, {Year}]")
    st.title("India Transactions Choropleth Map")

    if tab1:
        # Transaction Choropleth MAP : START--------------------------------------------------
        query_df_map = f"select State, sum(Transaction_Count) as All_Transactions, sum(Transaction_Amount) as Total_Payment_Value, " \
               f"(sum(Transaction_Amount)/sum(Transaction_Count)) as Avg_Transaction_Value" \
               f" from map_trans where Year={Year} and Quarter={Quarter} group by State;"
        df_map = pd.read_sql_query(query_df_map, mydb)
        map = folium.Map(location=[20, 80], zoom_start=4, scrollWheelZoom=False, tiles='CartoDB positron')

        choropleth = folium.Choropleth(
            geo_data='Projects/Phonepe_Pulse/states_india.geojson',
            name="Indian Map",
            data=df_map,
            columns=["State", "All_Transactions"],
            key_on='feature.properties.st_nm',
            line_opacity=0.8,
            highlight=True
        )
        choropleth.geojson.add_to(map)

        choropleth.geojson.add_child(folium.features.GeoJsonTooltip(['st_nm'], labels=False)
                                     )

        st_map = st_folium(map, width=700, height=500)

        State_map = 'andaman-&-nicobar-islands'
        if st_map['last_active_drawing']:
            State_map = st_map['last_active_drawing']['properties']['st_nm']

        # Transaction MAP: END

        qry4 = f"select State, sum(Transaction_Count) as All_Transactions, sum(Transaction_Amount) as Total_Payment_Value, " \
               f"(sum(Transaction_Amount)/sum(Transaction_Count)) as Avg_Transaction_Value" \
               f" from map_trans where Year={Year} and Quarter={Quarter} group by State having State = '{State_map}';"
        df_state = pd.read_sql_query(qry4, mydb)
        st.dataframe(df_state)

        District = ""
        qry5 = f"select State, District, Transaction_Count as All_Transactions, Transaction_Amount as Total_Payment_Value, " \
               f"(Transaction_Amount/Transaction_Count) as Avg_Transaction_Value from map_trans where " \
               f"State = '{State_map}' and Year={Year} and Quarter={Quarter} "
               # f" and District = {District}"
        df_district = pd.read_sql_query(qry5, mydb)
        st.dataframe(df_district)

    st.subheader(":violet[Top 10 Transactions]")
    col1t, col2t = st.columns(2)
    with col1t:
        st.write(f"**:orange[_Top Transaction Pincode of {State}_]**")
        qry6 = f"select District, Transaction_Count, Transaction_Amount from top_trans_district " \
               f"where State = '{State}' and Year={Year} and Quarter={Quarter};"
        df_top_dist = pd.read_sql_query(qry6, mydb)
        st.dataframe(df_top_dist)

    with col2t:
        st.write(f"**:orange[_Top Transaction Pincode of {State}_]**")
        qry7 = f"select Pincode, Transaction_Count, Transaction_Amount from top_trans_pincode " \
               f"where State = '{State}' and Year={Year} and Quarter={Quarter};"
        df_top_pincode = pd.read_sql_query(qry7, mydb)
        st.dataframe(df_top_pincode)

with tab2:

    st.subheader(f"**:violet[INDIA User Details- Q{Quarter}, {Year}]**")

    qry1 = f" select sum(Registered_Users) as Total_Registered_Users, sum(App_Opens) as Total_App_Opens " \
           f"from agg_user_state_reg where Year < {Year} or Year={Year} and Quarter<={Quarter};"
    IndiaUsers = pd.read_sql_query(qry1, mydb)
    st.write(f"**:orange[Registered PhonePe users till Q{Quarter} {Year} :] :black[{round(IndiaUsers.iloc[0, 0])}]**")

    qry2 = f"select sum(App_Opens) as Total_App_Opens from agg_user_state_reg where" \
           f" Year = {Year} and Quarter = {Quarter};"
    st.write(f"**:orange[_PhonePe app opens in Q{Quarter} {Year} :_] :black[{round(IndiaTotal.iloc[0, 0])}]**")

    st.subheader(f"**:violet[Top Indian Users Registered by State, District, Pincode  in Q{Quarter}, {Year}]**")

    col1u, col2u, col3u = st.columns(3)
    with col1u:
        st.write(f"**:orange[_Top 10 states_]**")
        qry6 = f"select State, sum(Registered_Users) as Total_Registered_Users from top_user_district " \
               f"where Year = {Year} and Quarter = {Quarter} group by State " \
               f"order by Total_Registered_Users desc limit 10;"
        df_top_user_state = pd.read_sql_query(qry6, mydb)
        st.dataframe(df_top_user_state[['State', 'Total_Registered_Users']])

    with col2u:
        st.write(f"**:orange[_Top 10 districts_]**")
        qry6 = f"select District, sum(Registered_Users) as Total_Registered_Users from top_user_district " \
               f"where Year = {Year} and Quarter = {Quarter} group by District " \
               f"order by Total_Registered_Users desc limit 10;"
        df_top_user_dist = pd.read_sql_query(qry6, mydb)
        st.dataframe(df_top_user_dist)

    with col3u:
        st.write(f"**:orange[_Top 10 Pincode_]**")
        qry7 = f"select Pincode, sum(Registered_Users) as Total_Registered_Users from top_user_pincode " \
               f"where Year = {Year} and Quarter = {Quarter} group by Pincode " \
               f"order by Total_Registered_Users desc limit 10;"
        df_top_user_pincode = pd.read_sql_query(qry7, mydb)
        st.dataframe(df_top_user_pincode)


    st.subheader(f":violet[Map Details - Q{Quarter}, {Year}]")

    # Transaction Choropleth MAP : START--------------------------------------------------
    if tab2:
        qry_map = f"select State, sum(Registered_Users) as Total_Registered_Users, sum(App_Opens) as Total_App_Opens " \
                  f"from map_user where Year = {Year} and Quarter = {Quarter} group by State having State = '{State}';"
        df_user_state_map = pd.read_sql_query(qry_map, mydb)

        map_user = folium.Map(location=[20, 80], zoom_start=4, scrollWheelZoom=False, tiles='CartoDB positron')

        choropleth_user = folium.Choropleth(
            geo_data='Projects/Phonepe_Pulse/states_india.geojson',
            name="Indian Map",
            data=df_user_state_map,
            columns=["State", "Total_Registered_Users"],
            key_on='feature.properties.st_nm',
            line_opacity=0.8,
            highlight=True
        )
        choropleth_user.geojson.add_to(map_user)
        #
        choropleth.geojson.add_child(folium.features.GeoJsonTooltip(['st_nm'], labels=False)
                                     )

        st_map = st_folium(map, width=700, height=500)

        State_user_map = 'andaman-&-nicobar-islands'
        if st_map['last_active_drawing']:
            State_user_map = st_map['last_active_drawing']['properties']['st_nm']

        # Transaction MAP: END

        qry4 = f"select State, sum(Registered_Users) as Total_Registered_Users, sum(App_Opens) as Total_App_Opens " \
               f"from map_user where Year = {Year} and Quarter = {Quarter} group by State having State = '{State_user_map}';"
        df_user_state = pd.read_sql_query(qry4, mydb)
        st.dataframe(df_user_state)
        # District Data
        qry5 = f"select District, Registered_Users, App_Opens from map_user where Year = {Year} " \
                   f"and Quarter = {Quarter} and State = '{State_user_map}';"

        # qry5 = f"select State, District, Registered_Users, App_Opens from map_user where Year = {Year} " \
        #        f"and Quarter = {Quarter} and State = '{State}' and District = 'north and middle andaman district';"
        df_user_district = pd.read_sql_query(qry5, mydb)
        st.dataframe(df_user_district)
    #
    State_U = st.selectbox("**:green[Select State : ]**", states_option)
    st.subheader(f":violet[Top Districts and Pincodes Registered Users of {State_U} for Q{Quarter}, {Year}]")
    col1t, col2t = st.columns(2)

    with col1t:
        st.write(f"**:orange[_Top 10 districts of {State_U}_]**")
        qry9 = f"select District, sum(Registered_Users) as Total_Registered_Users from top_user_district " \
               f"where State = '{State_U}' and Year = {Year} and Quarter = {Quarter} group by District " \
               f"order by Total_Registered_Users desc limit 10;"
        df_top_user_dist = pd.read_sql_query(qry9, mydb)
        st.dataframe(df_top_user_dist)
    with col2t:
        st.write(f"**:orange[_Top 10 Pincodes of {State_U}_]**")
        qry10 = f"select Pincode, sum(Registered_Users) as Total_Registered_Users from top_user_pincode " \
               f"where State = '{State_U}' and Year = {Year} and Quarter = {Quarter} group by Pincode " \
               f"order by Total_Registered_Users desc limit 10;"
        df_top_user_dist = pd.read_sql_query(qry10, mydb)
        st.dataframe(df_top_user_dist)

query.close()
mydb.close()
