import prefect
import requests
import os
from prefect import flow,task

key = os.environ["api_key_geo"]
api_url =  "http://api.weatherapi.com/v1/current.json"

@task()
def get_weather(weather_dict):
    print ("returning temperarture in celsius")
    return weather_dict['current']['temp_c']

@task()
def get_region(weather_dictionary):
    # print ("region is {}").format(str(weather_dictionary['location']))
    return weather_dictionary['location']

@flow()
def get_weather_location(lat,long):
    print ("Accessing the API")
    r = requests.post(api_url,params={"key":key,"q":(lat,long)})
    weather_dict =  r.json()
    temperature =  get_weather(weather_dict)
    print ("temperature is {}".format(str(temperature)))
    print (get_region(weather_dict))
    

if __name__ == "__main__":
    get_weather_location(28.7,77.1)