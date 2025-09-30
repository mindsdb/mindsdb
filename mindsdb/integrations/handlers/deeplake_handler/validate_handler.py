#!/usr/bin/env python3
"""
Deep Lake Handler Validation Script

This script validates the Deep Lake handler implementation without
requiring external dependencies like pandas, numpy, etc.
"""

import os
import sys
import ast


def validate_file_structure():
    """Validate that all required files exist."""
    print("🔍 Validating File Structure...")

    handler_dir = os.path.dirname(os.path.abspath(__file__))
    required_files = {
        "__init__.py": "Handler registration",
        "__about__.py": "Metadata and version info",
        "connection_args.py": "Connection parameters",
        "deeplake_handler.py": "Main handler implementation",
        "requirements.txt": "Dependencies",
        "README.md": "Documentation",
        "icon.svg": "Handler icon",
        "tests/__init__.py": "Test package",
        "tests/test_deeplake_handler.py": "Unit tests",
    }

    missing_files = []
    for file, description in required_files.items():
        file_path = os.path.join(handler_dir, file)
        if os.path.exists(file_path):
            print(f"  ✅ {file} - {description}")
        else:
            print(f"  ❌ {file} - MISSING - {description}")
            missing_files.append(file)

    return len(missing_files) == 0


def validate_init_file():
    """Validate __init__.py structure."""
    print("\n🔍 Validating Handler Registration...")

    init_path = os.path.join(os.path.dirname(__file__), "__init__.py")

    try:
        with open(init_path, "r") as f:
            content = f.read()

        # Check for required imports and variables
        required_elements = [
            "HANDLER_TYPE",
            'name = "deeplake"',
            "type = HANDLER_TYPE.DATA",
            'title = "Deep Lake"',
            "Handler",
            "connection_args",
        ]

        for element in required_elements:
            if element in content:
                print(f"  ✅ {element}")
            else:
                print(f"  ❌ {element} - MISSING")
                return False

        return True

    except Exception as e:
        print(f"  ❌ Error reading __init__.py: {e}")
        return False


def validate_connection_args():
    """Validate connection_args.py structure."""
    print("\n🔍 Validating Connection Arguments...")

    args_path = os.path.join(os.path.dirname(__file__), "connection_args.py")

    try:
        with open(args_path, "r") as f:
            content = f.read()

        # Check for required arguments
        required_args = [
            "dataset_path",
            "token",
            "search_default_limit",
            "search_distance_metric",
            "create_embedding_dim",
        ]

        for arg in required_args:
            # Check for various formats: "arg", 'arg', arg=
            patterns = [f'"{arg}"', f"'{arg}'", f"{arg}="]
            if any(pattern in content for pattern in patterns):
                print(f"  ✅ {arg}")
            else:
                print(f"  ❌ {arg} - MISSING")
                return False

        # Check for OrderedDict usage
        if "OrderedDict" in content:
            print("  ✅ Uses OrderedDict")
        else:
            print("  ❌ OrderedDict - MISSING")
            return False

        return True

    except Exception as e:
        print(f"  ❌ Error reading connection_args.py: {e}")
        return False


def validate_handler_class():
    """Validate main handler class structure."""
    print("\n🔍 Validating Handler Class...")

    handler_path = os.path.join(os.path.dirname(__file__), "deeplake_handler.py")

    try:
        with open(handler_path, "r") as f:
            content = f.read()

        # Parse the AST to check class structure
        tree = ast.parse(content)

        # Find the DeepLakeHandler class
        handler_class = None
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef) and node.name == "DeepLakeHandler":
                handler_class = node
                break

        if not handler_class:
            print("  ❌ DeepLakeHandler class not found")
            return False

        print("  ✅ DeepLakeHandler class found")

        # Check for required methods
        required_methods = [
            "__init__",
            "connect",
            "disconnect",
            "check_connection",
            "get_tables",
            "create_table",
            "insert",
            "select",
            "delete",
            "get_columns",
        ]

        class_methods = [node.name for node in handler_class.body if isinstance(node, ast.FunctionDef)]

        for method in required_methods:
            if method in class_methods:
                print(f"  ✅ {method} method")
            else:
                print(f"  ❌ {method} method - MISSING")
                return False

        # Check inheritance
        if handler_class.bases:
            base_names = []
            for base in handler_class.bases:
                if isinstance(base, ast.Name):
                    base_names.append(base.id)
                elif isinstance(base, ast.Attribute):
                    base_names.append(base.attr)

            if "VectorStoreHandler" in base_names:
                print("  ✅ Inherits from VectorStoreHandler")
            else:
                print("  ❌ Does not inherit from VectorStoreHandler")
                return False

        return True

    except Exception as e:
        print(f"  ❌ Error validating handler class: {e}")
        return False


def validate_requirements():
    """Validate requirements.txt."""
    print("\n🔍 Validating Requirements...")

    req_path = os.path.join(os.path.dirname(__file__), "requirements.txt")

    try:
        with open(req_path, "r") as f:
            content = f.read().strip()

        if "deeplake" in content:
            print("  ✅ Deep Lake dependency specified")

            # Check version specification
            if ">=" in content:
                print("  ✅ Version constraint specified")
            else:
                print("  ⚠️ No version constraint (recommended)")

            return True
        else:
            print("  ❌ Deep Lake dependency missing")
            return False

    except Exception as e:
        print(f"  ❌ Error reading requirements.txt: {e}")
        return False


def validate_tests():
    """Validate test structure."""
    print("\n🔍 Validating Tests...")

    test_path = os.path.join(os.path.dirname(__file__), "tests", "test_deeplake_handler.py")

    try:
        with open(test_path, "r") as f:
            content = f.read()

        # Check for test class
        if "class TestDeepLakeHandler" in content:
            print("  ✅ Test class found")
        else:
            print("  ❌ Test class missing")
            return False

        # Check for key test methods
        test_methods = [
            "test_01_handler_initialization",
            "test_02_connection_success",
            "test_07_create_table",
            "test_08_insert_data",
            "test_09_select_regular_query",
            "test_10_select_vector_search",
        ]

        found_tests = 0
        for method in test_methods:
            if method in content:
                found_tests += 1

        if found_tests >= 4:
            print(f"  ✅ {found_tests} test methods found")
        else:
            print(f"  ❌ Only {found_tests} test methods found (need at least 4)")
            return False

        # Check for mocking
        if "unittest.mock" in content or "Mock" in content:
            print("  ✅ Uses mocking for isolation")
        else:
            print("  ❌ No mocking found")
            return False

        return True

    except Exception as e:
        print(f"  ❌ Error validating tests: {e}")
        return False


def validate_documentation():
    """Validate README.md."""
    print("\n🔍 Validating Documentation...")

    readme_path = os.path.join(os.path.dirname(__file__), "README.md")

    try:
        with open(readme_path, "r") as f:
            content = f.read()

        required_sections = [
            "# Deep Lake Handler",
            "## Usage",
            "## Connection Parameters",
            "## Example Queries",
        ]

        for section in required_sections:
            if section in content:
                print(f"  ✅ {section}")
            else:
                print(f"  ❌ {section} - MISSING")
                return False

        # Check for SQL examples
        if "```sql" in content:
            print("  ✅ Contains SQL examples")
        else:
            print("  ❌ No SQL examples found")
            return False

        return True

    except Exception as e:
        print(f"  ❌ Error reading README.md: {e}")
        return False


def main():
    """Run all validation checks."""
    print("🧪 Deep Lake Handler Validation")
    print("=" * 50)

    checks = [
        ("File Structure", validate_file_structure),
        ("Handler Registration", validate_init_file),
        ("Connection Arguments", validate_connection_args),
        ("Handler Class", validate_handler_class),
        ("Requirements", validate_requirements),
        ("Tests", validate_tests),
        ("Documentation", validate_documentation),
    ]

    results = {}
    for name, check_func in checks:
        results[name] = check_func()

    print("\n" + "=" * 50)
    print("📊 Validation Summary:")

    passed = 0
    total = len(checks)

    for name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"  {status} - {name}")
        if result:
            passed += 1

    print(f"\n🏆 Overall: {passed}/{total} checks passed")

    if passed == total:
        print("🎉 Deep Lake Handler validation successful!")
        return True
    else:
        print("⚠️ Some validation checks failed.")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
