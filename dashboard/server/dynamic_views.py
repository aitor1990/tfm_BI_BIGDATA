import plotly.graph_objs as go



def bar_chart(dimension,fact,country):
    return {
        'data': [
                {'x': dimension, 'y': fact, 'type': 'bar', 'name': 'Cities'},
        ],
        'layout': {'title': country ,'margin':{'t':30,'l':30,'r':30,'b':60,'pad':4}}
    }

def evolution_chart(values):
    lines = []
    for key in values:
        lines +=[{'x':values[key]['years'], 'y': values[key]['facts'], 'type': 'scatter', 'name': key}]

    return {
        'data': lines,
        'layout': {'margin':{'t':30,'l':30,'r':30,'b':60,'pad':4}}
    }

def europe_map(countries,values):
    scl = [[0.0, 'rgb(242,240,247)'],[0.2, 'rgb(218,218,235)'],[0.4, 'rgb(188,189,220)'],\
                [0.6, 'rgb(158,154,200)'],[0.8, 'rgb(117,107,177)'],[1.0, 'rgb(84,39,143)']]

    #MAPA EUROPA
    data = [{
            'type':'choropleth',
            #colorscale : scl,
            #autocolorscale = False,
            'autocolorscale' : True,
            'locations' : countries,
            'z' : values,
            'showscale' : False,
             'marker': {
                'line': {
                    'color' : 'rgb(255,255,255)',
                    'width' : 2
                } },
            #colorbar = dict(
            #    title = "Variable name")
              }]

    layout = {
                #title = 'Europe',
                'geo': {
                    'scope':'europe',
                    'showlakes': True,
                    'lakecolor' : 'rgb(255, 255, 255)'
                },
                'margin':{'t':10,'l':10,'r':10,'b':10,'pad':2}
            }

    return {'data':data, 'layout':layout }
