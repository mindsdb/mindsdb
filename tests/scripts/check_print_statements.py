import glob
import re
import sys

success = True


def check_for_print_statements():
    global success

    files = list(
        set(glob.glob("**/*.py", recursive=True))
        - set(glob.glob("**/tests/**", recursive=True))
        - set(glob.glob("docker/**", recursive=True))
        - set(["mindsdb/__main__.py"])
    )

    pattern = re.compile("\sprint\(")  # noqa: W605

    failed_files = []

    for file in files:
        with open(file, "r") as fh:
            if len(re.findall(pattern, fh.read())) > 0:
                failed_files.append(file)

    if failed_files:
        success = False
        print("-- The following files contain print statements. Please remove them: --")
        print()
        for file in failed_files:
            print(file)


check_for_print_statements()

sys.exit(0 if success else 1)
