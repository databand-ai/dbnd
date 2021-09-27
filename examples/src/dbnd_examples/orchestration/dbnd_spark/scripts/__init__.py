from dbnd import relative_path


def spark_script(*path):
    return relative_path(__file__, *path)
