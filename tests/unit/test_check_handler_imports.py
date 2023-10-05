import ast
import importlib
import logging
import os
import unittest
from pathlib import Path

PACKAGE_ROOT = Path(__file__).parent.parent.parent.resolve()
HANDLERS_ROOT = PACKAGE_ROOT / "mindsdb" / "integrations" / "handlers"
MAIN_REQUIREMENTS_PATH = PACKAGE_ROOT / "requirements" / "requirements.txt"

# Set up logging
logging.basicConfig(level=logging.WARNING)


class TestPackageImports(unittest.TestCase):
    def test_imports_are_valid(self):
        all_imports_valid = True
        for root, _, files in os.walk(HANDLERS_ROOT):
            for file in files:
                if (
                    file.endswith(".py")
                    and file != "__init__.py"
                    and file != "setup.py"
                ):
                    module_path = os.path.join(root, file)
                    if not self.check_module_imports(module_path, root):
                        all_imports_valid = False

        self.assertTrue(
            all_imports_valid, "Some modules have invalid imports. Check the logs."
        )

    def check_module_imports(self, module_path, root_dir):
        with open(module_path, "r") as f:
            node = ast.parse(f.read())

        imports = [
            n for n in ast.walk(node) if isinstance(n, (ast.Import, ast.ImportFrom))
        ]
        for imp in imports:
            try:
                if isinstance(imp, ast.Import):
                    for name in imp.names:
                        importlib.import_module(name.name)
                elif isinstance(imp, ast.ImportFrom):
                    importlib.import_module(imp.module)
            except ImportError as e:
                missing_package = str(e).split("No module named ")[-1].strip("'")
                local_requirements_path = os.path.join(root_dir, "requirements.txt")
                if not (
                    self.is_in_requirements(missing_package, local_requirements_path)
                    or self.is_in_requirements(missing_package, MAIN_REQUIREMENTS_PATH)
                ):
                    logging.warning(f"Invalid import in {module_path}: {e}")
                    return False
            except Exception as e:
                logging.warning(f"Issue with {module_path}: {e}")
                return False
        return True

    def is_in_requirements(self, package, filepath):
        if not os.path.exists(filepath):
            return False
        with open(filepath, "r") as f:
            # Extract package names, ignoring comments and version specifiers
            required_packages = [
                line.split("==")[0].split(">")[0].split("<")[0].strip()
                for line in f
                if not line.startswith("#") and line.strip()
            ]
            return package in required_packages


if __name__ == "__main__":
    unittest.main()
