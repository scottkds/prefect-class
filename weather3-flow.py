import httpx
from prefect import flow, task, get_run_logger, serve
from prefect.tasks import task_input_hash
from prefect.artifacts import create_markdown_artifact
from prefect.events import emit_event
from datetime import datetime, timedelta


@task(retries=3, retry_delay_seconds=1, persist_result=True, cache_key_fn=task_input_hash, cache_expiration=timedelta(minutes=1))
def get_current_weather(lat: float = 38.9, lon: float = -77.0):
    logger = get_run_logger()
    logger.info(f"Getting weather for Latitude: {lat:0.4f}. Longitude: {lon:0.4f}")
    base_url = "https://api.open-meteo.com/v1/forecast/"
    weather = httpx.get(
        base_url,
        params=dict(latitude=lat, longitude=lon, hourly="temperature_2m"),
    )
    most_recent_temp = float(weather.json()["hourly"]["temperature_2m"][0])
    return f"Most recent temp C: {most_recent_temp} degrees"

@task
def mark_it_down(current_weather):
    markdown = f"# Weather in Kirkland: {current_weather}"
    create_markdown_artifact(key="current-weather", markdown=markdown, description="meteo api result")

@task
def save_weather(current_temp: str):
    with open("temps.txt", "a") as f:
        f.write(current_temp + "\n")


@flow
def fetch_weather_scott2():
    current_weather = get_current_weather(47.7097472, -122.1891462)
    save_weather(current_weather)
    mark_it_down(current_weather)
    print(current_weather)
    emit_event(
        event="This_is_an_event",
        resource={"prefect.resource.id": "NoIdHere"},
        )



if __name__ == "__main__":
    flow1 = fetch_weather_scott2.to_deployment(name="scooter-weather-1")
    serve(flow1)