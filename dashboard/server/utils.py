def europe_map(countries,values):
    scl = [[0.0, 'rgb(242,240,247)'],[0.2, 'rgb(218,218,235)'],[0.4, 'rgb(188,189,220)'],\
                [0.6, 'rgb(158,154,200)'],[0.8, 'rgb(117,107,177)'],[1.0, 'rgb(84,39,143)']]

    #MAPA EUROPA
    data = [ dict(
            type='choropleth',
            #colorscale = scl,
            #autocolorscale = False,
            autocolorscale = True,
            locations = countries,
            z = values,
            marker = dict(
                line = dict (
                    color = 'rgb(255,255,255)',
                    width = 2
                ) ),
            colorbar = dict(
                title = "Variable name")
            ) ]

    layout = dict(
            title = 'Europe',
            geo = dict(
                scope='europe',
                showlakes = True,
                lakecolor = 'rgb(255, 255, 255)'),
                 )

    return dict( data=data, layout=layout )
