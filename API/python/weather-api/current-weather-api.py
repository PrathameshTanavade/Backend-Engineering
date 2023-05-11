

#----------------

#City Coordinates dictionary




#-----------------------------------------------------------------------------------------------------------------


#-----------------------------------------------------------------------------------------------------------------

#CODE

import requests
import pandas as pd
from fastapi import FastAPI


app = FastAPI()



def weather(city):
    latitude=city_coordinates[city]['lat']
    longitude=city_coordinates[city]['lng']
    
    parameters={"latitude":latitude, "longitude":longitude}
    
    meteo_weather_api_url=url='https://api.open-meteo.com/v1/forecast?current_weather=true'
    
    meteo_weather_api_request=requests.get(meteo_weather_api_url,parameters)
    
    return meteo_weather_api_request.json()
    


@app.get("/current-weather/")
async def city(city:str):
    city=city.lower()  
    return weather(city)


#------------------------------------------------------------------------------------------------------------------