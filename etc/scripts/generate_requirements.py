from collections import defaultdict
from zipfile import ZipFile

from packaging.requirements import Requirement


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

        meta_field = meta_field[len("Requires-Dist:") :]
        req = Requirement(meta_field)
        if third_party_only and req.name.startswith("dbnd"):
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
            if str(req.marker).startswith("extra"):
                print("Extras marker:", req.marker)
                requirements_extras[str(req.marker)].append(req_str)
                continue
            req_str = "; ".join([req_str, str(req.marker)])
        requirements.append(req_str)

    for extra_name in extra_packages:
        extras = requirements_extras.get('extra == "%s"' % extra_name, [])
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
