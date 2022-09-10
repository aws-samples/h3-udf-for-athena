#!/usr/bin/env python
# coding: utf-8

# Begin by installing some Python modules that aren't included in the standard SageMaker environment.

# In[ ]:


get_ipython().system('pip install geojson')
get_ipython().system('pip install awswrangler')
get_ipython().system('pip install geomet')
get_ipython().system('pip install shapely')


# Next, import all of the packages that we'll need later.

# In[22]:


from geomet import wkt
import plotly.express as px
from shapely.geometry import Polygon, mapping
import awswrangler as wr
import pandas as pd
from shapely.wkt import loads
import geojson
import ast


# Use [AWS Python Data Wrangler](https://github.com/awslabs/aws-data-wrangler) to read earthquake data from an Athena table.
# 
# Data Wrangler is a very easy way to read data from various AWS data sources into Pandas dataframes.
# 
# For this visualisation, we're only interested in earthquakes [within the USA](https://en.wikipedia.org/wiki/List_of_extreme_points_of_the_United_States). So, filter out rows in the dataframe which are outside of the extremities.

# In[29]:


def run_query(lambda_arn, db, resolution):
    # Query is in two pieces to avoid Bandit error.
    query = f"""USING EXTERNAL FUNCTION cell_to_boundary_wkt(cell VARCHAR)
                    RETURNS ARRAY(VARCHAR)
                    LAMBDA '{lambda_arn}'
                       SELECT h3_cell, cell_to_boundary_wkt(h3_cell) as boundary, quake_count FROM(
                        USING EXTERNAL FUNCTION lat_lng_to_cell_address(lat DOUBLE, lng DOUBLE, res INTEGER)
                         RETURNS VARCHAR
                        LAMBDA '{lambda_arn}'
                    SELECT h3_cell, COUNT(*) AS quake_count"""
    query += f"""     FROM
                        (SELECT *,
                           lat_lng_to_cell_address(latitude, longitude, {resolution}) AS h3_cell
                         FROM earthquakes
                         WHERE latitude BETWEEN 18 AND 70        -- For this visualisation, we're only interested in earthquakes within the USA.
                           AND longitude BETWEEN -175 AND -50
                         )
                       GROUP BY h3_cell ORDER BY quake_count DESC) cell_quake_count"""
    return wr.athena.read_sql_query(query, database=db)

lambda_arn = '<MY-LAMBDA-ARN>' # Replace with ARN of your lambda.
db_name = '<MY-DATABASE-NAME>' # Replace with name of your Glue database.
earthquakes_df = run_query(lambda_arn=lambda_arn,db=db_name, resolution=4)
earthquakes_df.head()


# The next step is to make a GeoJSON-like Python dictionary that contains hexagons for each of the H3 cells that we know there were earthquakes in.
# 
# As a first step, this function converts a H3 cell name to a GeoJSON geometry dictionary.

# In[24]:


def wkt_point_to_coord(wkt_p):
    p = loads(wkt_p)
    return p.x, p.y

def geometry_dict(boundary):
    wkt_points = [wkt_point_to_coord(p) for p in boundary]
    wkt_string = Polygon(wkt_points).wkt
    
    geometry_string = geojson.dumps(mapping(loads(wkt_string)))

    geometry_dict = ast.literal_eval(geometry_string)

    return geometry_dict

def feature_dict(h3_id, boundary):
    geo_dict = {'type': 'Feature',
                'properties': {},
                'geometry': geometry_dict(boundary),          # The function defined above.
                'id': h3_id
                }
    
    return geo_dict


# Create a GeoJSON dictionary containing all of the H3 cells that have earthquakes in them.

# In[25]:


features_array = []

for _, row in earthquakes_df.iterrows():
    features_array.append(feature_dict(row['h3_cell'], row['boundary']))
    
h3_geojson = {'type': 'FeatureCollection',
              'features': features_array
             }

# Have a look at the first feature in the feature list.
print(h3_geojson['features'][0])


# Now it is time to visualise the data as a [cloropleth map](https://en.wikipedia.org/wiki/Choropleth_map), colouring in each hexagon according to the number of earthquakes that have occured inside it.
# 
# At this point, we have,
# * A dataframe with columns `h3_cell` and `quake_count`.
# * A GeoJSON string containing an array of features. Each feature is a H3 hexagon, including the coordinates of the vertices of the hexagon. Each hexagon has a key called `id`, which matches each `h3_cell` in the dataframe.
# 
# The [Plotly Express](https://plotly.com/python/plotly-express/) module includes a [`choropleth_mapbox`](https://plotly.com/python/mapbox-county-choropleth/) method which combines a dataframe with a GeoJSON and an underlying real-world map.

# In[26]:


fig = px.choropleth_mapbox(data_frame=earthquakes_df, 
                           geojson=h3_geojson, 
                           locations='h3_cell', 
                           color='quake_count',
                           color_continuous_scale='Rainbow',
                           mapbox_style='carto-positron',
                           zoom=5, 
                           center = {'lat': 36.1699, 'lon': -115.1398},
                           opacity=0.5,
                           labels={'h3_cell': 'H3 Cell', 'quake_count':'Number of earthquakes'}
                          )
fig.update_layout(margin={'r':0, 't':0, 'l':0, 'b':0})
fig.show()


# In[ ]:




