from requests_dao import getDimensionValues,getFactByCountryName,getAggFactByCountry,getDimensionValuesList
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
        value='all',
        style={'width': '40%', 'display': 'inline-block'}
    )]),

    html.Div(dcc.RangeSlider(
            id='year_slider',
            min=0,
            max=(len(years)-1),
            marks={i: years[i] for i in range(0,len(years))},
            value=[0,(len(years)-1)],
    ),style={'marginBottom': 50, 'marginTop': 25,'marginLeft': 20, 'marginRight': 20}),
    dcc.Graph(
                id="map",
                 style={'height': 500},
                figure = europe_map(result['dimension_aux'],result['fact'])
    ),
    dcc.Graph(
        id='example-graph',
        figure={
            'data': [
                {'x': result['dimension'], 'y': result['fact'], 'type': 'bar', 'name': 'Countries'},
            ],
            'layout': {'title': ''}
        }
    )


])

@app.callback(
    dash.dependencies.Output('example-graph', 'figure'),
    [dash.dependencies.Input('country_selector', 'value'),
    dash.dependencies.Input('fact_selector', 'value'),
    dash.dependencies.Input('year_slider', 'value')])
def update_figure(country,fact,year):
    result = getFactByCountryName(fact,years[year[0]],years[year[1]],country,numberRows = 10)
    return bar_chart(result['dimension'],result['fact'],result['dimension'])

@app.callback(
    dash.dependencies.Output('map', 'figure'),
    [dash.dependencies.Input('country_selector', 'value'),
    dash.dependencies.Input('fact_selector', 'value'),
    dash.dependencies.Input('year_slider', 'value')])
def update_map(country,fact,year):
    if not country or country == 'all' :
        result = getFactByCountryName(fact,years[year[0]],years[year[1]],country)
    else:
        result = getAggFactByCountry(fact,years[year[0]],years[year[1]],country)

    return europe_map(result['dimension_aux'],result['fact'])




if __name__ == '__main__':
    app.run_server(host='0.0.0.0',debug=True)
