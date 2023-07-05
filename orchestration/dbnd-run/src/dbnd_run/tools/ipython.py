# © Copyright Databand.ai, an IBM Company 2022


def show_run_url(run_url):
    import IPython

    html = 'Databand Run is <a href="%s" target="_blank" >at this link</a>' % run_url
    IPython.display.display(IPython.display.HTML(html))
