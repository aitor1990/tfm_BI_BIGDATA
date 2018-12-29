from requests_dao import *
import dash
import os
import dash_core_components as dcc
import dash_html_components as html
from plotly import graph_objs as go
from dynamic_views import *
from flask_caching import Cache
from views import *
from style import *

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app.css.append_css({'external_url': '/reset.css'})
app.server.static_folder = 'static'  # if you run app.py from 'root-dir-name' you don't need to specify.

cache = Cache()
cache.init_app(app.server, config={'CACHE_TYPE': 'simple'})

'''app.layout = html.Div(children=[
            html.H1(children='European cities'),
            html.Div([html.P("Topic"),datasetSelector], style = styleMarginCommon),
            html.Div([html.Div([html.P("Variables"),factSelector], style = styleMasterSelector),
            html.Div([html.P("Countries"),countrySelector], style = styleMasterSelector)]),
            html.Div([html.P("Year Interval"),rangeYearSelector], style=styleYearSelector),
            html.Div([mapGraph,barGraph],style= {'margin':'10px'})],
            style = {'backgroundColor': '#DCDCDC','padding':'20px', 'height':'1000px'})'''

app.layout = html.Div([
          html.Div([html.Img(src=app.get_asset_url("ue_icon.png"),style = europeanIconStyle),
                    html.H1(children='European cities', style = titleTextStyle)],style = titleDivStyle),
          html.Div([
                html.Div([
                        html.Div([html.Strong("Topic"),datasetSelector], style = styleMarginCommon),
                        html.Div([html.Div([html.Strong("Variables"),factSelector],style = styleMasterSelector),
                        html.Div([html.Strong("Countries"),countrySelector])],style = styleMasterSelector),
                        html.Div([html.Strong("Year Interval"),rangeYearSelector], style=styleYearSelector)],
                        style = selectorDivStyle),
                html.Div([mapGraph,barGraph,evolutionGraph],style = graphDivStyle)]
                ,style = contentDivStyle)
          ])


@app.callback(
    dash.dependencies.Output('bar-graph', 'figure'),
    [dash.dependencies.Input('country_selector', 'value'),
    dash.dependencies.Input('fact_selector', 'value'),
    dash.dependencies.Input('year_slider', 'value'),
    dash.dependencies.Input('group_facts_selector', 'value')])
def update_figure(country,fact,year,group):
    table = getTableFromTopic(group)
    result = getFactByCountryName(fact,years[year[0]],years[year[1]],country,numberRows = 10,table = table)
    return bar_chart(result['dimension'],result['fact'],result['dimension'])

@app.callback(
    dash.dependencies.Output('fact_selector', 'options'),
    [dash.dependencies.Input('group_facts_selector', 'value')])
def update_fact_selector(group):
    if group == 'tourism':
        return tourismVariables
    else:
        return labourVariables

@app.callback(
    dash.dependencies.Output('map', 'figure'),
    [dash.dependencies.Input('country_selector', 'value'),
    dash.dependencies.Input('fact_selector', 'value'),
    dash.dependencies.Input('year_slider', 'value'),
    dash.dependencies.Input('group_facts_selector', 'value')])
def update_map(country,fact,year,group):
    if group == 'tourism':
        table = TOURISM_FACTS_TABLE
    else:
        table = LABOUR_FACTS_TABLE
    if not country or country == 'all' :
        result = getFactByCountryName(fact,years[year[0]],years[year[1]],country,table = table)
    else:
        result = getAggFactByCountry(fact,years[year[0]],years[year[1]],country,table = table)

    return europe_map(result['dimension_aux'],result['fact'])


@app.callback(
    dash.dependencies.Output('evolution-graph', 'figure'),
    [dash.dependencies.Input('country_selector', 'value'),
    dash.dependencies.Input('fact_selector', 'value'),
    dash.dependencies.Input('year_slider', 'value'),
    dash.dependencies.Input('group_facts_selector', 'value')])
def updateEvolutionGraph(country,fact,year,group):
    table = getTableFromTopic(group)
    result = getFactByCountriesEvolution(fact, years[year[0]], years[year[1]], country, table = table)
    print(result)
    return evolution_chart(result)

if __name__ == '__main__':
    app.run_server(host='0.0.0.0',debug=True)
