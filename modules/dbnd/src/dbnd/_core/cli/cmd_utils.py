from dbnd._vendor import click


@click.command()
def ipython():
    """Get ipython shell with Databand's context"""
    # noinspection PyUnresolvedReferences
    from dbnd_web import models  # noqa
    from dbnd import new_dbnd_context
    from airflow.utils.db import create_session
    import IPython

    with new_dbnd_context(
        name="ipython", autoload_modules=False
    ) as ctx, create_session() as session:
        header = "\n\t".join(
            [
                "Welcome to \033[91mDataband\033[0m's ipython command.\nPredefined variable are",
                "\033[92m\033[1mctx\033[0m     -> dbnd_context",
                "\033[92m\033[1msession\033[0m -> DB session",
                "\033[92m\033[1mmodels\033[0m  -> dbnd models",
            ]
        )
        IPython.embed(colors="neutral", header=header)
