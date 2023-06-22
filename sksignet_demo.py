import pandas as pd 
import numpy as np
from matplotlib import pyplot 
import matplotlib.pyplot as plt

from IPython.display import HTML, display
import plotly.express as px

import plotly.graph_objects as go
from plotly.subplots import make_subplots

df_0 = pd.read_excel('C:/python_code/SKsignet/시연테이블_v0616.xlsx', sheet_name = '시연1_US')
df_1 = pd.read_excel('C:/python_code/SKsignet/시연테이블_v0616.xlsx', sheet_name = '시연2_막대그래프')
df_2 = pd.read_excel('C:/python_code/SKsignet/시연테이블_v0616.xlsx', sheet_name = '시연2_시간대별그래프')
df_3 = pd.read_excel('C:/python_code/SKsignet/시연테이블_v0616.xlsx', sheet_name = '시연2_Tag목록')
df_4 = pd.read_excel('C:/python_code/SKsignet/시연테이블_v0616.xlsx', sheet_name = '시연3_상세Tag')
# df_5 = pd.read_excel('/Users/a09921/Desktop/signet/sample/시연테이블_v0616.xlsx', sheet_name = '샘플데이터_노이즈추가_230608')
# df_6 = pd.read_excel('/Users/a09921/Desktop/signet/sample/시연테이블_v0616.xlsx', sheet_name = '샘플데이터_agg적용_230608')

def draw_map(df):
    fig = px.scatter_mapbox(df, lat="lat", lon="lon", hover_name="City", hover_data=["State", "Score"],
                            color = 'Score',
                            zoom=3, width=800, height=400)
    fig.update_layout(mapbox_style="open-street-map")
    fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
    return(fig)

draw_map(df_0).write_html("C:/python_code/SKsignet/df_0.html")


def draw_bar(df):
    fig = px.bar(df, x='Health_index', y='HW',width=500, height=350,text_auto='.2f',
            title="개별 충전기의 Health Index", range_x = [0, 1.2])
    fig.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)
    fig.add_vline(x=1.0, line_width=3, line_dash="dot", line_color="red")
    return(fig)

draw_bar(df_1).write_image('C:/python_code/SKsignet/df_1.png')