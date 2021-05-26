import json
from datetime import datetime as dt

import requests


def current_weather(api_key):
    APIKEY = api_key
    url = 'http://api.openweathermap.org/data/2.5/weather?q=Tbilisi&APPID={}&units=metric'.format(APIKEY)
    response = requests.get(url)
    data = json.loads(response.text)
    current = data['main']
    temp = current['temp']
    humidity = current['humidity']
    wind_speed = data['wind']['speed']
    weather = data['weather'][0]['description']
    return 'Current weather in Tbilisi: {}\nTemperature in degrees Celsius: {}\nHumidity: {}\nWind speed: {}'.format(weather, temp, humidity, wind_speed)


def find_time_v_1():
    x = dt.now()
    time_diff = 4
    return 'It is: {}:{}'.format(x.hour + time_diff, x.minute)


def find_time_v_2():
    x = dt.now()
    time_diff = 4
    return 'It is {} hours.'.format(x.hour + time_diff)
