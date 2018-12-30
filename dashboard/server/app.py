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
# if you run app.py from 'root-dir-name' you don't need to specify.
app.server.static_folder = 'static'

cache = Cache()
cache.init_app(app.server, config={'CACHE_TYPE': 'simple'})


app.layout = html.Div([
          html.Div([html.Img(src=app.get_asset_url("ue_icon.png"), style=europeanIconStyle),
                    html.H1(children='European cities', style=titleTextStyle)], style=titleDivStyle),
          html.Div([
                html.Div([
                        html.Div([html.Strong("Topic"), datasetSelector],
                                 style=styleMarginCommon),
                        html.Div([html.Div([html.Strong("Variables", style=textSelectorStyle), factSelector], style=styleMasterSelector),
                                  html.Div([html.Strong("Countries", style=textSelectorStyle), countrySelector, citySelector])], style=styleMasterSelector),
                                  html.Div(rangeYearSelector, style=styleYearSelector)],
                         #html.Div([html.Strong("Year Interval",style = textSelectorStyle), rangeYearSelector], style=styleYearSelector)],
                         style=selectorDivStyle),
                html.Div([mapGraph, barGraph, evolutionGraph], style=graphDivStyle)], style=contentDivStyle)
          ])


@app.callback(
    dash.dependencies.Output('bar-graph', 'figure'),
    [dash.dependencies.Input('country_selector', 'value'),
     dash.dependencies.Input('fact_selector', 'value'),
     dash.dependencies.Input('year_slider', 'value'),
     dash.dependencies.Input('group_facts_selector', 'value'),
     dash.dependencies.Input('city_selector', 'value')])
def update_bar_chart(country, fact, year, group, cities):
    table = getTableFromTopic(group)
    result = getFactByCountryName(
        fact, years[year[0]], years[year[1]], country, cityNames=cities, numberRows=10, table=table)
    return bar_chart(result['dimension'], result['fact'], result['dimension'])


@app.callback(
    dash.dependencies.Output('fact_selector', 'options'),
    [dash.dependencies.Input('group_facts_selector', 'value')])
def update_fact_selector(group):
    if group == 'tourism':
        return tourismVariables
    else:
        return labourVariables


@app.callback(
    dash.dependencies.Output('city_selector', 'options'),
    [dash.dependencies.Input('country_selector', 'value')])
def update_city_selector(country):
     if country == 'all' or country == '':
         return CITY_DEFAULT
     else:
         cities = getCitiesByCountry(country)
         response = []
         for city in cities:
             response += [{'label': city, 'value': city}]
         return response


@app.callback(
    dash.dependencies.Output('map', 'figure'),
    [dash.dependencies.Input('country_selector', 'value'),
     dash.dependencies.Input('fact_selector', 'value'),
     dash.dependencies.Input('year_slider', 'value'),
     dash.dependencies.Input('group_facts_selector', 'value')])
def update_map(country, fact, year, group):
    if group == 'tourism':
        table = TOURISM_FACTS_TABLE
    else:
        table = LABOUR_FACTS_TABLE
    if not country or country == 'all':
        result = getFactByCountryName(
            fact, years[year[0]], years[year[1]], country, table=table)
    else:
        result = getAggFactByCountry(
            fact, years[year[0]], years[year[1]], country, table=table)

    return europe_map(result['dimension_aux'], result['fact'])


@app.callback(
    dash.dependencies.Output('evolution-graph', 'figure'),
    [dash.dependencies.Input('country_selector', 'value'),
     dash.dependencies.Input('fact_selector', 'value'),
     dash.dependencies.Input('year_slider', 'value'),
     dash.dependencies.Input('group_facts_selector', 'value'),
     dash.dependencies.Input('city_selector', 'value')])
def updateEvolutionGraph(country, fact, year, group,cities):
    table = getTableFromTopic(group)
    result = getFactByCountriesEvolution(
        fact, years[year[0]], years[year[1]], country, table=table,cityNames = cities)
    return evolution_chart(result)


if __name__ == '__main__':
    app.run_server(host='0.0.0.0', debug=True)
