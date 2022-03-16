import json
import os
import re
import shutil
import sys

from dataclasses import dataclass

import requests


class Colors:
    OKGREEN = "\033[92m"
    ENDC = "\033[0m"
    YELLOW = "\033[33m"
    DARKGREEN = "\033[32m"
    RED = "\033[31m"


@dataclass
class Document:
    """Store basic information of a document."""

    slug: str
    title: str = ""
    order: int = 999
    hidden: bool = False
    id: str = None
    parent_slug: str = None
    parent_id: str = None


def _request_url(verb, url, payload=None):
    request = requests.request(method=verb, url=url, json=payload, headers=headers)
    _check_status_code(request)
    response = request.json()
    return response


def _check_status_code(request):
    if not request:
        print(f"{Colors.RED}Bad status code!{Colors.ENDC}")
        print(request.text)
        exit(1)


def _get_categories():
    url = "https://dash.readme.com/api/v1/categories"
    response = _request_url("GET", url)
    categories = [
        (x["slug"], x["_id"]) for x in response if x["slug"] in wanted_categories
    ]
    return categories


def _get_docs_from_category(category_slug: str):
    url = f"https://dash.readme.com/api/v1/categories/{category_slug}/docs"
    response = _request_url("GET", url)
    return response


def _get_doc_body(slug: str):
    url = f"https://dash.readme.com/api/v1/docs/{slug}"
    response = _request_url("GET", url)
    body = response["body"]
    return body


def _delete_doc(slug: str):
    url = f"https://dash.readme.com/api/v1/docs/{slug}"
    requests.delete(url, headers=headers)
    print(f"{Colors.YELLOW}Deleted page {slug}!{Colors.ENDC}")


def _update_doc(file_path: str, document: Document, category_id: str):
    with open(file_path, "r") as file:
        body = file.read()

    payload = {
        "hidden": document.hidden,
        "order": document.order,
        "title": document.title,
        "body": re.sub(page_info_pattern, "", body, re.DOTALL),
        "category": category_id,
    }
    if document.parent_id:
        payload["parentDoc"] = document.parent_id

    url = f"https://dash.readme.com/api/v1/docs/{document.slug}"
    page_info = re.match(page_info_pattern, body, re.DOTALL)

    if page_info:
        page_info = json.loads(f"{{{page_info.group(1)}}}")
        document.title = page_info["title"]
        payload["title"] = document.title

    _request_url("PUT", url, payload)
    print(f"{Colors.OKGREEN}Updated {document.slug}!{Colors.ENDC}")


def _create_doc(body: str, category: str, document: Document):
    payload = {
        "hidden": document.hidden,
        "order": 999,
        "category": category,
        "title": document.slug,
        "body": re.sub(page_info_pattern, "", body, re.DOTALL),
    }
    if document.parent_id:
        payload["parentDoc"] = document.parent_id

    url = "https://dash.readme.com/api/v1/docs"
    _request_url("POST", url, payload)

    page_info = re.match(page_info_pattern, body, re.DOTALL)
    document.title = document.slug
    if page_info:
        page_info = json.loads(f"{{{page_info.group(1)}}}")
        document.title = page_info["title"]
        payload["title"] = document.title
        url += f"/{document.slug}"
        _request_url("PUT", url, payload)
    print(f"{Colors.DARKGREEN}Created doc {document.title}!{Colors.ENDC}")


def fetch_category_docs(category_slug):
    if not os.path.exists(output_folder + category_slug):
        os.makedirs(output_folder + category_slug)

    response = _get_docs_from_category(category_slug)
    parent_docs = [Document(x["slug"], x["title"]) for x in response]
    child_docs = [
        Document(slug=x["slug"], parent_slug=y["slug"], title=x["title"])
        for y in response
        for x in y["children"]
    ]

    for parent_doc in parent_docs:
        body = f'---\n"title": "{parent_doc.title}"\n---\n'
        body += _get_doc_body(parent_doc.slug)
        with open(f"{output_folder}{category_slug}/{parent_doc.slug}.md", "w") as file:
            file.write(body)
        os.makedirs(f"{output_folder}{category_slug}/{parent_doc.slug}")
        print(f"{Colors.OKGREEN}Fetched {parent_doc.slug}!{Colors.ENDC}")

    for child_doc in child_docs:
        body = f'---\n"title": "{child_doc.title}"\n---\n'
        body += _get_doc_body(child_doc.slug)
        with open(
            f"{output_folder}{category_slug}/{child_doc.parent_slug}/{child_doc.slug}.md",
            "w",
        ) as file:
            file.write(body)
        print(f"{Colors.OKGREEN}Fetched {child_doc.slug}!{Colors.ENDC}")


def fetch_categories(slugs):
    if os.path.exists(output_folder):
        shutil.rmtree(output_folder)
    for category_slug in slugs:
        fetch_category_docs(category_slug)


def update_docs_in_category(category_slug, category_id, remove_nonexisting=False):
    if not os.path.exists(output_folder + category_slug):
        print(
            f"{Colors.RED}No {category_slug} folder in {os.path.abspath(output_folder)}!{Colors.ENDC}"
        )
        exit(1)

    response = _get_docs_from_category(category_slug)

    parent_docs = [
        Document(x["slug"], x["title"], x["order"], x["hidden"], x["_id"])
        for x in response
    ]  # main docs
    child_docs = [
        Document(
            x["slug"],
            x["title"],
            x["order"],
            x["hidden"],
            x["_id"],
            y["slug"],
            y["_id"],
        )
        for y in response
        for x in y["children"]
    ]

    for document in parent_docs:
        file_path = f"{output_folder}{category_slug}/{document.slug}"

        if not os.path.exists(file_path + ".md") and remove_nonexisting:
            if os.path.exists(file_path):
                shutil.rmtree(file_path)
            for child in child_docs:
                if child.parent_slug == document.slug:
                    _delete_doc(child.slug)

            _delete_doc(document.slug)
            continue

        _update_doc(file_path + ".md", document, category_id)

    for document in child_docs:
        file_path = (
            f"{output_folder}{category_slug}/{document.parent_slug}/{document.slug}.md"
        )
        if not os.path.exists(file_path) and remove_nonexisting:
            _delete_doc(document.slug)
            continue

        _update_doc(file_path, document, category_id)


def add_docs_to_category(category_slug, category_id):
    response = _get_docs_from_category(category_slug)
    parent_docs = [x["slug"] for x in response]
    parent_ids = {x["slug"]: x["_id"] for x in response}
    category_path = f"{output_folder}{category_slug}/"
    for slug in os.listdir(category_path):
        if os.path.isdir(category_path + slug):
            continue
        clean_slug = slug[:-3]
        if not os.path.exists(category_path + clean_slug):
            os.makedirs(category_path + clean_slug)
        if not clean_slug in parent_docs:
            with open(category_path + slug, "r") as file:
                body = file.read()
            _create_doc(body, category_id, Document(clean_slug))
            response = _get_docs_from_category(category_slug)

            parent_docs = [x["slug"] for x in response]
            parent_ids = {x["slug"]: x["_id"] for x in response}

        for child_slug in os.listdir(category_path + clean_slug):
            url = f"https://dash.readme.com/api/v1/docs/{child_slug[:-3]}"
            child_response = requests.get(url, headers=headers)
            if child_response.status_code == 404:
                with open(category_path + clean_slug + "/" + child_slug, "r") as file:
                    body = file.read()
                _create_doc(
                    body,
                    category_id,
                    Document(child_slug[:-3], parent_id=parent_ids[clean_slug]),
                )


def update_categories(categories, remove_nonexisting=False):
    for (category_slug, category_id) in categories:
        update_docs_in_category(category_slug, category_id, remove_nonexisting)
        add_docs_to_category(category_slug, category_id)


if (
    __name__ == "__main__"
):  # to run sync, in terminal do ```py sdk_documentation.py <apikey> <version(default latest)>
    page_info_pattern = r"---\n(.+?)\n---\n"
    argv = sys.argv
    if len(argv) < 2:
        print("Invalid number of parameters!")
        exit(1)
    apikey = argv[1]
    output_folder = "./sdk-docs/"
    wanted_categories = ["tracking-features", "orchestration-features"]
    headers = {"Accept": "application/json", "Authorization": apikey}
    if len(argv) > 2 and argv[2] != "default":
        headers["x-readme-version"] = argv[2]

    categories = _get_categories()
    update_categories(categories, True)
