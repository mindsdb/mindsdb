# Checks that handler requirements.txt and the main requirements.txt dont contain any of the same packages.

import glob
import re
import sys

pattern = '\=|~|>|<| |\n|#'  # noqa: W605
main_reqs_path = "requirements/requirements.txt"
success = True

with open(main_reqs_path, 'r') as main_f:
    main_reqs = [
        re.split(pattern, line)[0]
        for line in main_f.readlines()
        if re.split(pattern, line)[0]
    ]

reqs_files = glob.glob("**/requirements*.txt", recursive=True)
reqs_files.remove(main_reqs_path)

for file in reqs_files:
    with open(file, 'r') as fh:
        lines = [
            re.split(pattern, line)[0]
            for line in fh.readlines()
            if re.split(pattern, line)[0]
        ]

    for req in lines:
        if req in main_reqs:
            print(f"{req} is in {file} and also in main reqs")
            success = False

sys.exit(0 if success else 1)
