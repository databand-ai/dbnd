from collections import defaultdict
from zipfile import ZipFile

from packaging.requirements import Requirement


def _get_metadata(wheel_file):
    archive = ZipFile(wheel_file)
    for f in archive.namelist():
        if f.endswith("METADATA"):
            return archive.open(f).read().decode("utf-8")
    raise Exception("Metadata file not found in %s" % wheel_file)


def generate_requirements(
    wheel_file, requirements_file, extra_packages, third_party_only=False
):
    metadata = _get_metadata(wheel_file)
    requirements = []
    requirements_extras = defaultdict(list)
    for meta_field in metadata.split("\n"):
        if not meta_field.startswith("Requires-Dist:"):
            continue

        meta_field = meta_field[len("Requires-Dist:") :]
        req = Requirement(meta_field)
        if third_party_only and req.name.startswith("dbnd"):
            print("Skipping %s" % req.name)
            continue

        print(req)
        parts = [req.name]
        if req.extras:
            parts.append("[{0}]".format(",".join(sorted(req.extras))))
        req_str = "".join(parts)
        if req.marker:
            requirements_extras[str(req.marker)].append(req_str)
        else:
            requirements.append(req_str)

    for extra_name in extra_packages:
        for p in requirements_extras.get('extra == "%s"' % extra_name, []):
            if p not in requirements:
                requirements.append(p)

    print("Requirements:\n\t%s" % "\n\t".join(requirements))
    with open(requirements_file, "w") as rg:
        rg.write("\n".join(requirements))


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Extract wheel dependencies.")
    parser.add_argument(
        "--wheel", dest="wheel", required=True, help="Path to wheel file"
    )
    parser.add_argument(
        "--output", dest="output", required=True, help="Path to output requires.txt"
    )
    parser.add_argument(
        "--extras", dest="extras", default="", help="Extras (comma separated)"
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
        requirements_file=args.output,
        extra_packages=args.extras.split(","),
        third_party_only=args.third_party_only,
    )


if __name__ == "__main__":
    main()
