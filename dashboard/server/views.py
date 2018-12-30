import dash_core_components as dcc
from requests_dao import *
import plotly.graph_objs as go
from style import *


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
    style=factSelectorStyle,
    value='beds'
)

countrySelector = dcc.Dropdown(
    id='country_selector',
    multi=False,
    options=getDimensionValues('country_name'),
    value='',
    style=countrySelectorStyle
)
CITY_DEFAULT = [{'label':'all','value':''}]
citySelector = dcc.Dropdown(
    id='city_selector',
    multi=True,
    options=CITY_DEFAULT,
    value='',
    style=countrySelectorStyle
)

years = getDimensionValuesListYear('year', 'tourism_facts')
rangeYearSelector = dcc.RangeSlider(
        id='year_slider',
        min=0,
        max=(len(years)-1),
        marks={i: years[i] for i in range(0, len(years))},
        value=[0, (len(years)-1)],
        vertical=True
)

mapGraph = dcc.Graph(
        id="map",
        style=mapGraphStyle
)

barGraph = dcc.Graph(
        id='bar-graph',
        style=barGraphStyle
)

evolutionGraph = dcc.Graph(
        id='evolution-graph',
        style=evolutionGraphStyle
)

tourismVariables = [{'label': 'beds', 'value': 'beds'},
                    {'label': 'cinemas seats', 'value': 'cinema_seats'},
                    {'label': 'nights spent', 'value': 'nights'}]

labourVariables = [{'label': 'activity rate', 'value': 'activity_rate'},
                   {'label': 'activiy rate female',
                       'value': 'activity_rate_female'},
                   {'label': 'activiy rate male', 'value': 'activity_rate_male'},
                   {'label': 'unemployment rate ', 'value': 'unem_rate'},
                   {'label': 'unemployment rate female',
                       'value': 'unem_rate_female'},
                   {'label': 'unemployment rate male', 'value': 'unem_rate_male'},
                   {'label': 'employment rate agriculture',
                       'value': 'empl_agriculture'},
                   {'label': 'employment rate industry', 'value': 'empl_industry'},
                   {'label': 'employment rate construction', 'value': 'empl_construction'}]
