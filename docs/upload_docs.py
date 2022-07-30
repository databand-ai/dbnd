# © Copyright Databand.ai, an IBM Company 2022

import json
import sys

import requests


category_slug = "dbnd-module"  # `dbnd-module` in case of the main dbnd docs


def update_doc(slug, title, order, hidden, parent=""):
    url = f"https://dash.readme.com/api/v1/docs/{slug}"
    with open(
        f"./_build/html/reference/{'api/dbnd.' if parent else ''}{slug}.html"
    ) as file:
        body = r"""
    [block:html]
    {{
      "html": "{}"
    }}
    [/block]""".format(
            repr(file.read())[1:-1]
            .replace('"', '\\"')
            .replace(r"\xa0", " ")
            .replace(r"'", r"\'")
            .replace(r"\\'", r"'")
        )
    payload = {
        "hidden": hidden,
        "order": order,
        "title": title,
        "type": "basic",
        "body": body,
        "category": category_id,
    }
    if parent:
        payload.update({"parentDoc": parent})

    response = requests.request("PUT", url, json=payload, headers=headers)
    if response.status_code == 200:
        print(f"\033[92mSuccessfully updated {title}!\033[0m")
    else:
        print(f"Doc {slug} failed uploading")
        print(response.text)
        print(f"Body:\n{body}")
        exit()


if __name__ == "__main__":
    argv = sys.argv
    if len(argv) != 2:
        print("Invalid number of arguments! Enter only apikey as an argument!")
        exit(1)
    key = argv[1]
    headers = {"Accept": "application/json", "Authorization": key}

    url = f"https://dash.readme.com/api/v1/categories/{category_slug}"
    category_response = json.loads(requests.request("GET", url, headers=headers).text)
    category_id = category_response["_id"]

    get_docs_url = f"https://dash.readme.com/api/v1/categories/{category_slug}/docs"
    response = json.loads(requests.request("GET", get_docs_url, headers=headers).text)
    parent_docs = [
        [x["slug"], x["title"], x["order"], x["hidden"]] for x in response
    ]  # all main pages
    child_docs = [
        [x["slug"], x["title"], x["order"], y["_id"], x["hidden"]]
        for y in response
        for x in y["children"]
    ]  # all children of the main pages
    for [slug, title, order, hidden] in parent_docs:
        update_doc(slug, title, order, hidden)
    for [slug, title, order, parent, hidden] in child_docs:
        update_doc(slug, title, order, hidden, parent)
