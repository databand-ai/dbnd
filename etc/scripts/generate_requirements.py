# © Copyright Databand.ai, an IBM Company 2022

from collections import defaultdict
from zipfile import ZipFile

from packaging.markers import _format_marker
from packaging.requirements import Requirement as BaseRequirement


class Requirement(BaseRequirement):
    """
    Custom Requirement class with some shortcuts

    # doctest: `python -m doctest -v dbnd-core/etc/scripts/generate_requirements.py`

    >>> req = Requirement('Requires-Dist: future (==0.16.0) ; (python_version < "3.0") and extra == "tests"')
    >>> req.is_dbnd_package()
    False
    >>> req.get_extra_marker()
    'tests'
    >>> req.get_markers_str_without_extra()
    'python_version < "3.0"'
    >>> req = Requirement('Requires-Dist: dbnd-airflow (<5.0.0,>=4.2) ; (python_version >= "3.0") and (python_version <= "3.6")')
    >>> req.is_dbnd_package()
    True
    >>> req.get_extra_marker() is None
    True
    >>> req.get_markers_str_without_extra()
    'python_version >= "3.0" and python_version <= "3.6"'
    """

    def __init__(self, requirement_string):
        if requirement_string.startswith("Requires-Dist:"):
            requirement_string = requirement_string[len("Requires-Dist:") :]
        super(Requirement, self).__init__(requirement_string)

    def is_dbnd_package(self):
        return self.name.startswith("dbnd")

    def get_extra_marker(self):
        if self.marker:
            for expr in self.marker._markers:
                if not isinstance(expr, tuple):
                    continue
                variable, op, value = expr
                if variable.value == "extra":
                    return value.value
        return None

    def get_markers_str_without_extra(self):
        if self.marker:
            markers_without_extra = []
            for expr in self.marker._markers:
                if isinstance(expr, tuple):
                    variable, op, value = expr
                    if variable.value == "extra":
                        # previous marker might be 'and' and now it's redundant
                        if markers_without_extra:
                            if markers_without_extra.pop(-1) == "and":
                                pass  # everything as expected
                            else:
                                # we don't have such cases ATM but they might apper
                                raise NotImplementedError()
                        continue
                markers_without_extra.append(expr)
            return _format_marker(markers_without_extra)
        return None


def _get_metadata(wheel_file):
    archive = ZipFile(wheel_file)
    for f in archive.namelist():
        if f.endswith("METADATA"):
            return archive.open(f).read().decode("utf-8")
    raise Exception("Metadata file not found in %s" % wheel_file)


def save_to_file(requirements_file, requirements):
    print(
        "Saving requirements to %s:\n\t%s"
        % (requirements_file, "\n\t".join(requirements))
    )
    with open(requirements_file, "w") as rg:
        rg.write("\n".join(requirements))


def generate_requirements(
    wheel_file,
    requirements_file,
    extra_packages,
    separate_extras=False,
    third_party_only=False,
):
    metadata = _get_metadata(wheel_file)
    requirements = []
    requirements_extras = defaultdict(list)
    for meta_field in metadata.split("\n"):
        if not meta_field.startswith("Requires-Dist:"):
            continue

        req = Requirement(meta_field)
        if third_party_only and req.is_dbnd_package():
            print("Skipping %s" % req.name)
            continue

        print(req)

        parts = [req.name]
        if req.extras:
            parts.append("[{0}]".format(",".join(sorted(req.extras))))

        if req.specifier:
            parts.append(str(req.specifier))
        req_str = "".join(parts)
        if req.marker:
            extra_marker = req.get_extra_marker()
            if extra_marker:
                print("Extras marker:", req.marker)
                req_parts = filter(bool, [req_str, req.get_markers_str_without_extra()])
                requirements_extras[extra_marker].append("; ".join(req_parts))
                continue
            req_str = "; ".join([req_str, str(req.marker)])
        requirements.append(req_str)

    for extra_name in extra_packages:
        extras = requirements_extras.get(extra_name, [])
        if separate_extras and extras:
            extras_filename = requirements_file + "[%s]" % extra_name
            if ".requirements.txt" in requirements_file:
                extras_filename = requirements_file.replace(
                    ".requirements.txt", "[%s].requirements.txt" % extra_name
                )

            save_to_file(extras_filename, extras)
            continue
        for p in extras:
            if p not in requirements:
                requirements.append(p)

    save_to_file(requirements_file, requirements)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Extract wheel dependencies.")
    parser.add_argument(
        "--wheel", dest="wheel", required=True, help="Path to wheel file"
    )
    parser.add_argument(
        "--output", dest="output", required=False, help="Path to output requires.txt"
    )
    parser.add_argument(
        "--extras", dest="extras", default="", help="Extras (comma separated)"
    )
    parser.add_argument(
        "--separate-extras",
        dest="separate_extras",
        action="store_true",
        help="Save extras into separate requirement files",
    )
    parser.add_argument(
        "--third-party-only",
        dest="third_party_only",
        action="store_true",
        help="Show only third party deps",
    )

    args = parser.parse_args()

    generate_requirements(
        wheel_file=args.wheel,
        requirements_file=args.output
        or args.wheel.replace(".whl", ".requirements.txt"),
        extra_packages=args.extras.split(","),
        separate_extras=args.separate_extras,
        third_party_only=args.third_party_only,
    )


if __name__ == "__main__":
    main()
