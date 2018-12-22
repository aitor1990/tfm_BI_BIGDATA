# -*- coding: utf-8 -*-
import dash
import dash_core_components as dcc
import dash_html_components as html
from pydrill.client import PyDrill
from jinja2 import Template
from plotly import graph_objs as go



beds_by_country = '''select avg(b.beds) as average_bed ,a.city_name
from dfs.`/data/city_dimension.parquet` a,dfs.`/data/tourism_facts.parquet` b
where a.index_city = b.index_city and a.country_name = '{{ country_name }}' group by a.city_name'''

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

drill = PyDrill(host='drill', port=8047)
if not drill.is_active():
    raise ImproperlyConfigured('Please run Drill first')

results_query = drill.query('''select a.country_name from dfs.`/data/city_dimension.parquet` a''')
results = []
for result in results_query:
    resultValue = {}
    resultValue['value'] = result['country_name']
    resultValue['label'] = result['country_name']
    results += [resultValue]

scl = [[0.0, 'rgb(242,240,247)'],[0.2, 'rgb(218,218,235)'],[0.4, 'rgb(188,189,220)'],\
            [0.6, 'rgb(158,154,200)'],[0.8, 'rgb(117,107,177)'],[1.0, 'rgb(84,39,143)']]
            
#MAPA EUROPA
data = [ dict(
        type='choropleth',
        colorscale = scl,
        autocolorscale = False,
        locations = ['FRA', 'DEU', 'RUS', 'ESP'],
        z = [1,2,3,4],
        marker = dict(
            line = dict (
                color = 'rgb(255,255,255)',
                width = 2
            ) ),
        colorbar = dict(
            title = "Millions USD")
        ) ]

layout = dict(
        title = 'Europe',
        geo = dict(
            scope='europe',
            showlakes = True,
            lakecolor = 'rgb(255, 255, 255)'),
             )

fig = dict( data=data, layout=layout )

app.layout = html.Div(children=[
    html.H1(children='Hello Dash'),

    html.Div(children='''
        Dash: A web application framework for Python.
    '''),

    dcc.Dropdown(
        id='country_selector',
        options=results,
        multi=False,
        value="MTL"
    ),

    dcc.Graph(
        id='example-graph',
        figure={
            'data': [
                {'x': [1, 2, 3], 'y': [4, 1, 2], 'type': 'bar', 'name': 'SF'},
                {'x': [1, 2, 3], 'y': [2, 4, 5], 'type': 'bar', 'name': u'Montréal'},
            ],
            'layout': {
                'title': 'Dash Data Visualization'
            }
        }
    ),
    dcc.Graph(
                id="map",
                style={"height": "90%", "width": "98%"},
                figure = fig
    )
])

@app.callback(
    dash.dependencies.Output('example-graph', 'figure'),
    [dash.dependencies.Input('country_selector', 'value')])
def update_figure(country):
    results_query = drill.query(Template(beds_by_country).render(country_name=country))
    cities = []
    results = []

    for result in results_query:
        cities += [result['city_name']]
        results += [result['average_bed']]

    figure = {
        'data': [
            {'x': cities, 'y':results, 'type': 'bar', 'name': u'Countries'},
        ],
        'layout': {'title': country }
    }
    return figure



if __name__ == '__main__':
    app.run_server(host='0.0.0.0',debug=True)
