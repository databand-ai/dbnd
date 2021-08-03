import time

from dbnd import pipeline, task


@task()
def sleep_task(sleep_interval: int):
    while True:
        print(f"Going to sleep for {sleep_interval} seconds! wake me up later...")
        time.sleep(sleep_interval)
        print("Oh no! I overslept")


@pipeline()
def sleepy(sleep_interval: int):
    sleep_task(sleep_interval)
