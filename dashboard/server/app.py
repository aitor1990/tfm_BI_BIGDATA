#from requests_dao import getDimensionValues,getFactByCountryName,getAggFactByCountry,getDimensionValuesList
from requests_dao import *
import dash
import dash_core_components as dcc
import dash_html_components as html
from plotly import graph_objs as go
from utils import europe_map,bar_chart
from flask_caching import Cache


external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
cache = Cache()
cache.init_app(app.server, config={'CACHE_TYPE': 'simple'})


years = getDimensionValuesList('year','tourism_facts')
result = getFactByCountryName('beds',years[0],years[(len(years)-1)],countryName = '',aggOperation='avg')


app.layout = html.Div(children=[
    html.H1(children='European cities dashboard'),

    html.Div([dcc.RadioItems(
        id='group_facts_selector',
        options=[
            {'label': 'Labour', 'value':'labour'},
            {'label': 'Tourism', 'value': 'tourism'}
        ],
        value='tourism'
    )]),
    html.Div(children='Variables'),
    html.Div([dcc.Dropdown(
        id='fact_selector',
        options=[{'label': 'beds', 'value': 'beds'},
        {'label': 'cinemas seats', 'value': 'cinema_seats'},
        {'label': 'nights spent', 'value': 'nights'}],
        multi=False,
        value="beds",
        style={'width': '40%', 'display': 'inline-block'}
    ),dcc.Dropdown(
        id='country_selector',
        options=getDimensionValues('country_name'),
        multi=False,
        value="all",
        style={'width': '40%', 'display': 'inline-block'}
    )]),

    html.Div(dcc.RangeSlider(
            id='year_slider',
            min=0,
            max=(len(years)-1),
            marks={i: years[i] for i in range(0,len(years))},
            value=[0,(len(years)-1)],
    ),style={'marginBottom': 50, 'marginTop': 25,'marginLeft': 20, 'marginRight': 20}),
    html.Div([
        dcc.Graph(
                    id="map",
                    style={'width': '50%', 'display': 'inline-block'},
                    #style={'height': 500},
                    figure = europe_map(result['dimension_aux'],result['fact'])
        ),
        dcc.Graph(
            id='example-graph',
            figure={
                'data': [
                    {'x': result['dimension'], 'y': result['fact'], 'type': 'bar', 'name': 'Countries'},
                ],
                'layout': {'title': ''}
            },
            style={'width': '50%', 'display': 'inline-block'}
        )
    ]),



])

@app.callback(
    dash.dependencies.Output('example-graph', 'figure'),
    [dash.dependencies.Input('country_selector', 'value'),
    dash.dependencies.Input('fact_selector', 'value'),
    dash.dependencies.Input('year_slider', 'value'),
    dash.dependencies.Input('group_facts_selector', 'value')])
def update_figure(country,fact,year,group):
    if group == 'tourism':
        table = TOURISM_FACTS_TABLE
    else :
        table = LABOUR_FACTS_TABLE
    result = getFactByCountryName(fact,years[year[0]],years[year[1]],country,numberRows = 10,table = table)
    return bar_chart(result['dimension'],result['fact'],result['dimension'])

@app.callback(
    dash.dependencies.Output('fact_selector', 'options'),
    [dash.dependencies.Input('group_facts_selector', 'value')])
def update_fact_selector(group):
    if group == 'tourism':
        return [{'label': 'beds', 'value': 'beds'},
        {'label': 'cinemas seats', 'value': 'cinema_seats'},
        {'label': 'nights spent', 'value': 'nights'}]
    else :
        return [{'label': 'activity rate', 'value': 'activity_rate'},
                {'label': 'activiy rate female', 'value': 'activity_rate_female'},
                {'label': 'activiy rate male', 'value': 'activity_rate_male'},
                {'label': 'unemployment rate ', 'value': 'unem_rate'},
                {'label': 'unemployment rate female', 'value': 'unem_rate_female'},
                {'label': 'unemployment rate male', 'value': 'unem_rate_male'},
                {'label': 'employment rate agriculture', 'value': 'empl_agriculture'},
                {'label': 'employment rate industry', 'value': 'empl_industry'},
                {'label': 'employment rate construction', 'value': 'empl_construction'}]

@app.callback(
    dash.dependencies.Output('map', 'figure'),
    [dash.dependencies.Input('country_selector', 'value'),
    dash.dependencies.Input('fact_selector', 'value'),
    dash.dependencies.Input('year_slider', 'value'),
    dash.dependencies.Input('group_facts_selector', 'value')])
def update_map(country,fact,year,group):
    if group == 'tourism':
        table = TOURISM_FACTS_TABLE
    else :
        table = LABOUR_FACTS_TABLE
    if not country or country == 'all' :
        result = getFactByCountryName(fact,years[year[0]],years[year[1]],country,table = table)
    else:
        result = getAggFactByCountry(fact,years[year[0]],years[year[1]],country,table = table)

    return europe_map(result['dimension_aux'],result['fact'])




if __name__ == '__main__':
    app.run_server(host='0.0.0.0',debug=False)
