import dash_core_components as dcc
from requests_dao import *
import plotly.graph_objs as go


datasetSelector = dcc.RadioItems(
    id='group_facts_selector',
    options=[
        {'label': 'Labour', 'value': 'labour'},
        {'label': 'Tourism', 'value': 'tourism'}
    ],
    value='tourism'
)

factSelector = dcc.Dropdown(
    id='fact_selector',
    multi=False,
    style={'width': '90%'}
)

countrySelector = dcc.Dropdown(
    id='country_selector',
    multi=False,
    options=getDimensionValues('country_name'),
    value="all",
    style={'width': '100px'}
)

years = getDimensionValuesListYear('year','tourism_facts')
rangeYearSelector = dcc.RangeSlider(
        id='year_slider',
        min=0,
        max=(len(years)-1),
        marks={i: years[i] for i in range(0,len(years))},
        value=[0,(len(years)-1)]
)

mapGraph = dcc.Graph(
        id="map",
        style={'width': '45%', 'display': 'inline-block', 'float': 'left','marginLeft':'2.5%'}
)

barGraph = dcc.Graph(
        id='bar-graph',
        style={'width': '45%', 'display': 'inline-block', 'float': 'right', 'marginRight': '2.5%'}
)

evolutionGraph = dcc.Graph(
        id='evolution-graph',
        style={'display': 'inline-block', 'float': 'left', 'marginRight': '2.5%', 'marginLeft': '2.5%', 'width': '95%', 'marginTop': 25}
)

tourismVariables = [{'label': 'beds', 'value': 'beds'},
                    {'label': 'cinemas seats', 'value': 'cinema_seats'},
                    {'label': 'nights spent', 'value': 'nights'}]

labourVariables = [{'label': 'activity rate', 'value': 'activity_rate'},
        {'label': 'activiy rate female', 'value': 'activity_rate_female'},
        {'label': 'activiy rate male', 'value': 'activity_rate_male'},
        {'label': 'unemployment rate ', 'value': 'unem_rate'},
        {'label': 'unemployment rate female', 'value': 'unem_rate_female'},
        {'label': 'unemployment rate male', 'value': 'unem_rate_male'},
        {'label': 'employment rate agriculture', 'value': 'empl_agriculture'},
        {'label': 'employment rate industry', 'value': 'empl_industry'},
        {'label': 'employment rate construction', 'value': 'empl_construction'}]
