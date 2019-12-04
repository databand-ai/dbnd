def create_airflow_user(
    email,
    firstname,
    lastname,
    role,
    use_random_password,
    username,
    password="",
    hashed_password="",
):
    from dbnd_airflow.cli.cmd_airflow_db import _db_user_create as af_create_user
    from dbnd_airflow.web.airflow_app import cached_appbuilder as af_cached_appbuilder

    af_appbuilder = af_cached_appbuilder()
    af_user = af_appbuilder.sm.find_user(username, email)
    if not af_user:
        af_create_user(
            role=role.name,
            username=username,
            email=email,
            firstname=firstname,
            lastname=lastname,
            password=password,
            hashed_password=hashed_password,
            use_random_password=use_random_password,
        )
    else:
        af_user.password = password
        af_user.first_name = firstname
        af_user.last_name = lastname
        af_appbuilder.sm.update_user(af_user)
