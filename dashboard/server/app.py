# -*- coding: utf-8 -*-
import dash
import dash_core_components as dcc
import dash_html_components as html
from pydrill.client import PyDrill

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

drill = PyDrill(host='drill', port=8047)
if not drill.is_active():
    raise ImproperlyConfigured('Please run Drill first')

yelp_reviews = drill.query('''SELECT * FROM dfs.`/data/testout.parquet` LIMIT 5''')
results = []
for result in yelp_reviews:
    resultValue = {}
    resultValue['value'] = result['word']
    resultValue['label'] = result['word']
    results += [resultValue]
print(results)
app.layout = html.Div(children=[
    html.H1(children='Hello Dash'),

    html.Div(children='''
        Dash: A web application framework for Python.
    '''),


    dcc.Dropdown(
        options=results,
        multi=True,
        value="MTL"
    )
])

if __name__ == '__main__':
    app.run_server(host='0.0.0.0',debug=True)
