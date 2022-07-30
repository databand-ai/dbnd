# © Copyright Databand.ai, an IBM Company 2022

import json
import logging
import os
import re
import shutil

from dataclasses import dataclass

import click
import requests


version_suffix = "-internal"
version_main = False

OUTPUT_FOLDER = "./sdk-docs/"
PAGE_INFO_PATTERN = r"---\n(.+?)\n---\n"
WANTED_CATEGORIES = ["tracking-features", "orchestration-features"]
headers = {"Accept": "application/json"}

logger = logging.getLogger(__name__)
logging.basicConfig(level="INFO")


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
    request.raise_for_status()
    response = request.json()
    return response


def _get_versions():
    url = "https://dash.readme.com/api/v1/version"
    response = _request_url("GET", url)
    versions = [x["version"] for x in response]
    return versions


def _get_main_version():
    url = "https://dash.readme.com/api/v1/version"
    response = _request_url("GET", url)
    for version_object in response:
        if version_object["is_stable"]:
            return version_object["version"]


def _version_get_or_create(current_version, readme_versions):
    current_version = ".".join(
        current_version.split(".")[:-1]
    )  # Removes the patch number
    new_version = current_version + version_suffix
    current_version_exists = any(
        [new_version == version for version in readme_versions]
    )

    if current_version_exists:
        return new_version

    _create_version(new_version, _get_main_version())
    logger.info(f"Created new version {new_version}")
    return new_version


def _create_version(version_name: str, fork: str):
    url = "https://dash.readme.com/api/v1/version"
    payload = {
        "is_beta": not version_main,
        "version": version_name,
        "from": fork,
        "is_stable": version_main,
        "is_hidden": not version_main,
        "is_deprecated": False,
    }
    _request_url("POST", url, payload)


def _get_categories():
    url = "https://dash.readme.com/api/v1/categories"
    response = _request_url("GET", url)
    categories = [
        (x["slug"], x["_id"]) for x in response if x["slug"] in WANTED_CATEGORIES
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
    logger.info(f"{Colors.YELLOW}Deleted page {slug}!{Colors.ENDC}")


def _update_doc(file_path: str, document: Document, category_id: str):
    with open(file_path, "r") as file:
        body = file.read()

    payload = {
        "hidden": document.hidden,
        "order": document.order,
        "title": document.title,
        "body": re.sub(PAGE_INFO_PATTERN, "", body, re.DOTALL),
        "category": category_id,
    }
    if document.parent_id:
        payload["parentDoc"] = document.parent_id

    url = f"https://dash.readme.com/api/v1/docs/{document.slug}"
    page_info = re.match(PAGE_INFO_PATTERN, body, re.DOTALL)

    if page_info:
        page_info = json.loads(f"{{{page_info.group(1)}}}")
        document.title = page_info["title"]
        payload["title"] = document.title

    _request_url("PUT", url, payload)
    logger.info(f"{Colors.OKGREEN}Updated {document.slug}!{Colors.ENDC}")


def _create_doc(body: str, category: str, document: Document):
    payload = {
        "hidden": document.hidden,
        "order": 999,
        "category": category,
        "title": document.slug,
        "body": re.sub(PAGE_INFO_PATTERN, "", body, re.DOTALL),
    }
    if document.parent_id:
        payload["parentDoc"] = document.parent_id

    url = "https://dash.readme.com/api/v1/docs"
    _request_url("POST", url, payload)

    page_info = re.match(PAGE_INFO_PATTERN, body, re.DOTALL)
    document.title = document.slug
    if page_info:
        page_info = json.loads(f"{{{page_info.group(1)}}}")
        document.title = page_info["title"]
        payload["title"] = document.title
        url += f"/{document.slug}"
        _request_url("PUT", url, payload)
    logger.info(f"{Colors.DARKGREEN}Created doc {document.title}!{Colors.ENDC}")


def fetch_category_docs(category_slug):
    if not os.path.exists(os.path.join(OUTPUT_FOLDER, category_slug)):
        os.makedirs(os.path.join(OUTPUT_FOLDER, category_slug))

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
        with open(
            os.path.join(OUTPUT_FOLDER, category_slug, f"{parent_doc.slug}.md"), "w"
        ) as file:
            file.write(body)
        os.makedirs(os.path.join(OUTPUT_FOLDER, category_slug, parent_doc.slug))
        print(f"{Colors.OKGREEN}Fetched {parent_doc.slug}!{Colors.ENDC}")

    for child_doc in child_docs:
        body = f'---\n"title": "{child_doc.title}"\n---\n'
        body += _get_doc_body(child_doc.slug)
        with open(
            os.path.join(
                OUTPUT_FOLDER,
                category_slug,
                child_doc.parent_slug,
                f"{child_doc.slug}.md",
            ),
            "w",
        ) as file:
            file.write(body)
        logger.info(f"{Colors.OKGREEN}Fetched {child_doc.slug}!{Colors.ENDC}")


def fetch_categories(slugs):
    if os.path.exists(OUTPUT_FOLDER):
        shutil.rmtree(OUTPUT_FOLDER)
    for category_slug in slugs:
        fetch_category_docs(category_slug)


def update_docs_in_category(category_slug, category_id, remove_nonexisting=False):
    if not os.path.exists(os.path.join(OUTPUT_FOLDER, category_slug)):
        logger.error(
            f"{Colors.RED}No {category_slug} folder in {os.path.abspath(OUTPUT_FOLDER)}!{Colors.ENDC}"
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
        file_path = os.path.join(OUTPUT_FOLDER, category_slug, document.slug)

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
        file_path = os.path.join(
            OUTPUT_FOLDER, category_slug, document.parent_slug, f"{document.slug}.md"
        )
        if not os.path.exists(file_path) and remove_nonexisting:
            _delete_doc(document.slug)
            continue

        _update_doc(file_path, document, category_id)


def add_docs_to_category(category_slug, category_id):
    response = _get_docs_from_category(category_slug)
    parent_docs = [x["slug"] for x in response]
    parent_ids = {x["slug"]: x["_id"] for x in response}
    category_path = os.path.join(OUTPUT_FOLDER, category_slug)
    for slug in os.listdir(category_path):
        if os.path.isdir(os.path.join(category_path, slug)):
            continue
        clean_slug = slug[:-3]
        if not os.path.exists(os.path.join(category_path, clean_slug)):
            os.makedirs(os.path.join(category_path, clean_slug))
        if not clean_slug in parent_docs:
            with open(os.path.join(category_path, slug), "r") as file:
                body = file.read()
            _create_doc(body, category_id, Document(clean_slug))
            response = _get_docs_from_category(category_slug)

            parent_docs = [x["slug"] for x in response]
            parent_ids = {x["slug"]: x["_id"] for x in response}

        for child_slug in os.listdir(os.path.join(category_path, clean_slug)):
            url = f"https://dash.readme.com/api/v1/docs/{child_slug[:-3]}"
            child_response = requests.get(url, headers=headers)
            if child_response.status_code == 404:
                with open(
                    os.path.join(category_path, clean_slug, child_slug), "r"
                ) as file:
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


def main(apikey, target_version):
    headers["Authorization"] = apikey
    existing_versions = _get_versions()
    used_version = _version_get_or_create(target_version, existing_versions)
    logger.info(f"Using version {used_version}")
    headers["x-readme-version"] = used_version

    categories = _get_categories()
    update_categories(categories, True)


@click.command()
@click.option("--api-key", required=True, help="Our Readme website apikey")
@click.option("--version", default="0.1.1", help="Current DBND build version")
@click.option(
    "--is-main",
    default=False,
    is_flag=True,
    help="Specified version should be the main version",
)
@click.option(
    "--suffix", default="", help="What suffix should be added to the version number"
)
def upload_docs(api_key, version, is_main, suffix):
    global version_main, version_suffix
    version_main = is_main
    version_suffix = suffix
    main(api_key, version)


if __name__ == "__main__":
    upload_docs()
