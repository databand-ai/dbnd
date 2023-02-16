# © Copyright Databand.ai, an IBM Company 2022

import os
import re


def clean_links(path_to_file):
    if path_to_file[-5:] == ".html":
        with open(f"_build/html/reference/{path_to_file}", "r") as file:
            html = file.read()
            html = re.sub(
                r"href=\"(?:api\/)?(?:dbnd\.)?(\w+)\.html(?:#dbnd\.\w+)?\"(>)?",
                lambda x: f'href="{x.group(1).lower()}"{x.group(2)}',
                html,
                0,
                re.MULTILINE,
            )
        with open(f"_build/html/reference/{path_to_file}", "w") as file:
            file.write(html)


if __name__ == "__main__":
    os.system("sphinx-build -M clean . _build")
    os.system("sphinx-build -M html . _build -W")
    for name in os.listdir("_build/html/reference"):
        clean_links(name)
        os.rename(
            f"_build/html/reference/{name}", f"_build/html/reference/{name.lower()}"
        )
    for name in os.listdir("_build/html/reference/api"):
        clean_links(f"api/{name}")
        os.rename(
            f"_build/html/reference/api/{name}",
            f"_build/html/reference/api/{name.lower()}",
        )
    print("\n\n\033[92mBuild is finished! See the results at docs/_build/html!\033[0m")
