# © Copyright Databand.ai, an IBM Company 2022

from dbnd import band, parameter, task


@task
def extract(source=parameter.data):
    return None


@task(sketch=parameter.data)
def read_update(sketch, events, apps):
    return None


@task
def write(source, dest):
    return None


@band
def update_sketch_strait_band(sketch, daily, apps, dest=None):
    extracted = extract(source=daily)
    updated_sketch = read_update(sketch=sketch, events=extracted, apps=apps)
    if not dest:
        dest = sketch
    write_mongo = write(source=updated_sketch, dest=dest)

    return write_mongo
