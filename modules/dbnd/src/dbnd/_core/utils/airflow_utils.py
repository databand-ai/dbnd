from typing import List, Optional


def get_project_name_from_airflow_tags(tags):
    # type: (Optional[List[str]]) -> Optional[str]
    if not tags:
        return None

    project_tag_prefix = "project:"
    for tag in sorted(tags):
        if tag.startswith(project_tag_prefix):
            return tag.replace(project_tag_prefix, "").strip() or None
