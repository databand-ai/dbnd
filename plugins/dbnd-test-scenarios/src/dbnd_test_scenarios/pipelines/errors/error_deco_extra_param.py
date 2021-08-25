from dbnd import band


@band(ssss=3)
def band_with_extra_param():
    return None
