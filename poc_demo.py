from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, mean as mean_, listagg, count

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from streamlit_plotly_events import plotly_events
from plotly.subplots import make_subplots

connection_parameters = {
    "account": "cp64151.ap-northeast-2.aws",
    "user": "skccadmin",
    "password": "QWer12#$%",
    "role": "ACCOUNTADMIN",
    "warehouse": "WH_XS",
    "database": "POC_DW_DB",
    "schema": "POC_DW_DB_SCHEMA"    
}

new_session = Session.builder.configs(connection_parameters).create()

# Loading Location data from Snowflake
loc_df = new_session.table("LOCATION").select("STATE", "CITY", "STATION_NUMBER", "LAT", "LON", "SCORE")
loc_pd_df = loc_df.to_pandas()
health_inx_df = new_session.table("HW_HEALTH_INDEX")
health_inx_pd_df = health_inx_df.to_pandas()
inx_detail_df = new_session.table("HW_INDEX_DETAIL")
inx_detail_pd_df = inx_detail_df.to_pandas()
measure_df = new_session.table("MEASUREMENT")
measure_pd_df = measure_df.to_pandas()
measuer_inx_df = new_session.table("MEASUREMENT_INDEX_DETAIL")
measuer_inx_pd_df = measuer_inx_df.to_pandas()
charger_inx_df = new_session.table("CHARGER_HEALTH_INDEX")
charger_inx_pd_df = charger_inx_df.to_pandas()

# Function for drawing map, made by DS2
def draw_map(df):
    fig = px.scatter_mapbox(df, lat="LAT", lon="LON", hover_name="CITY", hover_data=["STATE", "SCORE"],
                            color = 'SCORE', 
                            color_continuous_scale=px.colors.sequential.Plasma_r, zoom=2.7)
                            #zoom=3, width=800, height=400)
    fig.update_layout(mapbox_style="open-street-map")
    fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
    return(fig)

# Function for drawing bar chart about health index , made by DS2
def draw_bar(df):
    fig = px.bar(df, x='HEALTH_INDEX', y='HW',width=500, height=350,text_auto='.2f',
            title="HW Health Index", range_x = [0, 1.2])

    fig.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)
    fig.add_vline(x=1.0, line_width=3, line_dash="dot", line_color="red")
    return(fig)

# Function for drawing line chart about health index monitoring, made by DS2
def draw_index_time(df): 
    df['TIME'] = pd.date_range('2023-06-11 23:58:00', '2023-06-24', freq='2min')[:df.shape[0]]
    cols = ['POWER_SOURCE', 'PCS', 'CONNECTORS_CABLE', 'CONTROL_SYSTEMS', 'EFFICIENCY', 'SAFETY' ]    
    fig = make_subplots(rows=6, shared_xaxes=False)
    fig.add_scatter(x  = df['TIME'], y = df[cols[0]], name = cols[0], row=1, col=1) 
    fig.add_scatter(x  = df['TIME'], y = df[cols[1]], name = cols[1], row=2, col=1) 
    fig.add_scatter(x  = df['TIME'], y = df[cols[2]], name = cols[2], row=3, col=1) 
    fig.add_scatter(x  = df['TIME'], y = df[cols[3]], name = cols[3], row=4, col=1) 
    fig.add_scatter(x  = df['TIME'], y = df[cols[4]], name = cols[4], row=5, col=1) 
    fig.add_scatter(x  = df['TIME'], y = df[cols[5]], name = cols[5], row=6, col=1) 
    fig.update_layout(title='HW Health Index Trend', height=1000)           
    return(fig)

# Function for drawing table about measurement, made by DS2
def draw_table(df):
    fig = go.Figure()
    fig.add_trace(go.Table(
        header=dict(values=['Measurements'],align="center", height=20),
        cells=dict(values= [df['MEASUREMENTS']], align="center",height=20)))
    return(fig)

# Function for drawing line chart about measurement index monitoring, made by DS2
def draw_measure_time(df): 
    df['TIME'] = pd.date_range('2023-01-01', '2023-06-24', freq='2min')[:df.shape[0]]
    cols = ['VALUE_TEMP', 'SCALED_ANOMALY_SCORE', 'SCALED_ANOMALY_THRESHOLD' ]    
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_scatter(x  = df['TIME'], y = df[cols[0]], name = cols[0], line_color = 'blue')
    fig.add_scatter(x  = df['TIME'], y = df[cols[1]], name = cols[1], line_color = 'gray', secondary_y=True)
    fig.add_scatter(x  = df['TIME'], y = df[cols[2]] ,name = cols[2], secondary_y=True,  line_dash="dot", line_color="red")
    fig.update_yaxes(secondary_y=True, range=[0, 1.2])
    fig.update_layout(title='Value Tracking of Measurement')           
    return(fig)

def highlight(row):    
    if row.SCORE >= 0.8:        
        return ['background-color: aquamarine']*3   
    else:        
        return ['background-color: white']*3 

   
# Map
# st.set_page_config(layout="wide")

st.subheader('Charging Staion Anormaly Score in the US')
loc_fig = draw_map(loc_pd_df)
map_selected = plotly_events(loc_fig, click_event=True, override_width="100%")
if (len(map_selected) != 0):
    val = map_selected[0]["pointIndex"]
    result_df = loc_pd_df.loc[[val]]
    # result_df.loc[val]['STATION_NUMBER'] = val
    result_df['STATION_NUMBER'] = val
    st.dataframe(result_df, use_container_width=True, hide_index=True)
    
    # dataframe = pd.DataFrame({
    #          #'CITY' : [result_df.iloc[0]['CITY'], result_df.iloc[0]['CITY'], result_df.iloc[0]['CITY'], result_df.iloc[0]['CITY'], result_df.iloc[0]['CITY']],
    #          'STATION_NUMBER': [408, 408, 408, 408, 408],
    #          'CHARGER_ID': ['Charger-A', 'Charger-B', 'Charger-C', 'Charger-D', 'Charger-E'],
    #          'SCORE': [0.59, 0.87, 0.16, 0.78, 0.96]
    # })

    if result_df.loc[val]['LON'] >= -98 :
        charger_df = charger_inx_pd_df[charger_inx_pd_df['STATION_NUMBER'] == 487]
    else:
        charger_df = charger_inx_pd_df[charger_inx_pd_df['STATION_NUMBER'] == 635]

    charger_df['STATION_NUMBER'] = val
    st.dataframe(charger_df.style.apply(highlight, axis = 1), hide_index=True)
    
    option = st.selectbox(
                 'Select Charger? 👉',
                 ('Not Selected', 'Charger-A', 'Charger-B', 'Charger-C', 'Charger-D', 'Charger-E'))
    
    # st.write('You selected:', option)
    
    if (option != 'Not Selected'):
        tab1, tab2, tab3 = st.tabs(["Summary", "Trend", "Measurements"])
        with tab1:
            health_inx_fig = draw_bar(health_inx_pd_df)
            st.plotly_chart(health_inx_fig, theme="streamlit", use_container_width=True)
        with tab2:
            inx_detail_fig = draw_index_time(inx_detail_pd_df)
            st.plotly_chart(inx_detail_fig, theme="streamlit", use_container_width=True)
        with tab3:
            # st.subheader('Values of Measurement')
            option2 = st.selectbox(
                 'Select Measurement?',
                 (measure_pd_df["MEASUREMENTS"]))
            # st.write('You selected:', option2)
            measure_inx_fig = draw_measure_time(measuer_inx_pd_df)
            st.plotly_chart(measure_inx_fig, theme="streamlit", use_container_width=True)    
                     
new_session.close()
