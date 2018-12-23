# -*- coding: utf-8 -*-
import dash
import dash_core_components as dcc
import dash_html_components as html
from pydrill.client import PyDrill
from jinja2 import Template
from plotly import graph_objs as go
from utils import europe_map


beds_by_country = '''
select avg(b.beds) as average_bed ,a.city_name
from dfs.`/data/city_dimension.parquet` a,dfs.`/data/tourism_facts.parquet` b
where a.index_city = b.index_city and a.country_name = '{{ country_name }}'
group by a.city_name
order by average_bed desc
'''

total_beds_by_country = '''
select avg(b.beds) as average_bed ,a.country_name, a.country_map_code
from dfs.`/data/city_dimension.parquet` a,dfs.`/data/tourism_facts.parquet` b
where a.index_city = b.index_city group by a.country_name, a.country_map_code
'''

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

drill = PyDrill(host='drill', port=8047)
if not drill.is_active():
    raise ImproperlyConfigured('Please run Drill first')

results_query = drill.query(total_beds_by_country)
results = []
countries = []
values = []
for result in results_query:
    resultValue = {}
    resultValue['value'] = result['country_name']
    resultValue['label'] = result['country_name']
    countries += [result['country_map_code']]
    values += [result['average_bed']]
    results += [resultValue]



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
                figure = europe_map(countries,values)
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
