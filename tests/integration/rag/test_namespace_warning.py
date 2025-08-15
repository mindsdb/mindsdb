import warnings, importlib, pkgutil, mindsdb
def test_no_model_namespace_warning():
    warnings.filterwarnings(
        "error",
        message=r'.*protected namespace "model_"',
        module=r'^mindsdb\..*'
    )
    skipped, checked = 0, 0
    for mod in pkgutil.walk_packages(mindsdb.__path__, 'mindsdb.'):
        try:
            importlib.import_module(mod.name)
            checked += 1       
        except Warning as w:
            raise AssertionError(
                f"Pydantic namespace warning raised when importing {mod.name}: {w}"
            ) from w
        except Exception:
            skipped += 1
            continue
    print(
        f"[namespace‑warning] imported {checked} modules, "
        f"skipped {skipped} optional‑dep modules."
    )
