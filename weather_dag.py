
from airflow import DAG
from datetime import timedelta, datetime, timezone
from airflow.providers.http.sensors.http import HttpSensor
import json
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import pandas as pd


def kelvin_to_celsius(temp_in_kelvin):
    temp_in_celsius = round(temp_in_kelvin - 273.15)
    return temp_in_celsius





def transform_load_data(task_instance):
    data = task_instance.xcom_pull(task_ids="extract_weather_data")
    city = data["name"]
    country = data['sys']['country']
    weather_description = data["weather"][0]['description']
    temp_celcius = kelvin_to_celsius(data["main"]["temp"])
    feels_like_celcius= kelvin_to_celsius(data["main"]["feels_like"])
    min_temp_celcius = kelvin_to_celsius(data["main"]["temp_min"])
    max_temp_celcius = kelvin_to_celsius(data["main"]["temp_max"])
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    cloud_coverage = data['clouds']['all']
    # Use timezone-aware datetime objects to convert to local time
    time_of_record = datetime.fromtimestamp(data['dt'] + data['timezone'], timezone.utc)
    sunrise_time = datetime.fromtimestamp(data['sys']['sunrise'] + data['timezone'], timezone.utc)
    sunset_time = datetime.fromtimestamp(data['sys']['sunset'] + data['timezone'], timezone.utc)
    transformed_data = {"City": city,
                        "Country": country,
                        "Description": weather_description,
                        "Temperature (C)": temp_celcius,
                        "Feels Like (C)": feels_like_celcius,
                        "Minimun Temp (C)":min_temp_celcius,
                        "Maximum Temp (C)": max_temp_celcius,
                        "Pressure": pressure,
                        "Humidty": humidity,
                        "Wind Speed": wind_speed,
                        "Cloud_coverage": cloud_coverage,
                        "Time of Record": time_of_record,
                        "Sunrise (Local Time)":sunrise_time,
                        "Sunset (Local Time)": sunset_time                        
                        }
    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)


    time_now = datetime.now()
    dt_string = time_now.strftime("%d%m%Y%H%M%S")
    dt_string = 'current_weather_data_Lagos_' + dt_string
    df_data.to_csv(f"{dt_string}.csv", index=False)




default_args = {
    'owner': '3Signet',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 30),
    'email': ['olawumisalaam@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}




# sensor to check if endpoint is ready
with DAG('weather_dag',
        default_args=default_args,
        schedule_interval = '@hourly',
        catchup=False) as dag:


        is_weather_api_ready = HttpSensor(
        task_id ='is_weather_api_ready',
        http_conn_id='weathermap_api',
        endpoint='/data/2.5/weather?q=Lagos,NG&appid=c48f2051480e680c230cbff1ddcb522f'
        )

        #https://api.openweathermap.org/data/2.5/weather?q=Lagos,NG&appid=c48f2051480e680c230cbff1ddcb522f


        # Extraction layer

        extract_weather_data = SimpleHttpOperator(
        task_id = 'extract_weather_data',
        http_conn_id = 'weathermap_api',
        endpoint='/data/2.5/weather?q=Lagos,NG&appid=c48f2051480e680c230cbff1ddcb522f',
        method = 'GET',
        response_filter= lambda r: json.loads(r.text),
        log_response=True
        )

        # transformation/cleaning layer

        transform_load_weather_data = PythonOperator(
        task_id = 'transform_load_weather_data',
        python_callable = transform_load_data
        )


        is_weather_api_ready >> extract_weather_data >> transform_load_weather_data
