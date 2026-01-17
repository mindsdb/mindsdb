
# Fix for LiteLLM support: map base_url to openai_api_base
if "base_url" in args and "openai_api_base" not in args:
    args["openai_api_base"] = args.pop("base_url")
