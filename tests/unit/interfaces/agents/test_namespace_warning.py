import warnings
import importlib


def test_no_model_namespace_warning_llmconfig_import_and_init():
    with warnings.catch_warnings():
        warnings.filterwarnings("error", message=r'.*protected namespace "model_"')
        mod = importlib.import_module("mindsdb.integrations.utilities.rag.settings")
        LLMConfig = getattr(mod, "LLMConfig")
        _ = LLMConfig()
