def to_serializable(obj):
    # Primitives
    if isinstance(obj, (str, int, float, bool, type(None))):
        return obj
    # Pydantic v2
    if hasattr(obj, "model_dump"):
        return to_serializable(obj.model_dump(exclude_none=True))
    # Pydantic v1
    if hasattr(obj, "dict"):
        return to_serializable(obj.dict(exclude_none=True))
    # Custom classes with __dict__
    if hasattr(obj, "__dict__"):
        return {k: to_serializable(v) for k, v in vars(obj).items() if not k.startswith("_")}
    # Dicts
    if isinstance(obj, dict):
        return {k: to_serializable(v) for k, v in obj.items()}
    # Lists, Tuples, Sets
    if isinstance(obj, (list, tuple, set)):
        return [to_serializable(v) for v in obj]
    # Fallback: string
    return str(obj)
