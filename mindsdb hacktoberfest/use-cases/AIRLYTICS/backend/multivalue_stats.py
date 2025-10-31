import pandas as pd
import numpy as np

# ============================================================
# 1️⃣ CONDITIONAL RATING ANALYSIS
# ============================================================

def conditional_rating_analysis(
    data: list[dict],
    conditional_field: str,
    rating_field: str,
    threshold: float = 5,
    operator: str = ">="
):
    """
    🔍 Conditional Rating Analysis
    ---------------------------------
    Analyzes how categorical or boolean conditional_field values distribute 
    across rating_field thresholds using flexible comparison operators.

    Supported Operators: [">", "<", ">=", "<=", "==", "!="]
    """

    if not isinstance(data, list) or len(data) == 0:
        return {"error": "Input 'data' must be a non-empty list of dictionaries."}

    df = pd.DataFrame(data)
    if conditional_field not in df.columns or rating_field not in df.columns:
        return {"error": f"Missing required field(s): {conditional_field}, {rating_field}"}

    df = df.dropna(subset=[rating_field])
    df = df[df[rating_field].apply(lambda x: isinstance(x, (int, float)))]
    if df.empty:
        return {"error": f"No valid numeric values found for '{rating_field}'."}

    ops = {
        ">": lambda a, b: a > b,
        "<": lambda a, b: a < b,
        ">=": lambda a, b: a >= b,
        "<=": lambda a, b: a <= b,
        "==": lambda a, b: a == b,
        "!=": lambda a, b: a != b
    }
    compare = ops.get(operator)
    if compare is None:
        return {"error": f"Invalid operator '{operator}'."}

    mask = compare(df[rating_field], threshold)
    inverse_mask = ~mask

    def summarize(sub_df):
        total = len(sub_df)
        if total == 0:
            return {"total": 0, "distribution": {}}
        counts = sub_df[conditional_field].value_counts(dropna=True)
        perc = (counts / total * 100).round(2)
        if len(perc) > 10:
            top = perc.head(10)
            others = 100 - top.sum()
            perc = pd.concat([top, pd.Series({"Others": round(others, 2)})])
        return {"total": int(total), "distribution": perc.to_dict()}

    return {
        "rating_field": rating_field,
        "conditional_field": conditional_field,
        "threshold": threshold,
        "operator": operator,
        "matching_group": summarize(df[mask]),
        "opposite_group": summarize(df[inverse_mask]),
        "visualization": "bar_chart",
        "note": "Flexible operator support with top-10 categorical trimming."
    }


# ============================================================
# 2️⃣ CONDITIONAL RATING-TO-RATING ANALYSIS (UPDATED)
# ============================================================

def conditional_rating_to_rating_analysis(
    data: list[dict],
    base_field: str,
    compare_field: str,
    base_operator: str = ">=",
    base_threshold: float = 5,
    compare_operator: str = ">=",
    compare_threshold: float = 3
):
    """
    🔢 Conditional Rating-to-Rating Analysis (Full Operator Support)
    -----------------------------------------------------------------
    Compares numeric fields with all operators [">", "<", ">=", "<=", "==", "!="].
    Example:
        "What percent of users whose overall_rating <= 5 had seat_comfort >= 3?"
    """

    if not isinstance(data, list) or len(data) == 0:
        return {"error": "Input 'data' must be a non-empty list of dictionaries."}

    df = pd.DataFrame(data)
    missing = [f for f in [base_field, compare_field] if f not in df.columns]
    if missing:
        return {"error": f"Missing required field(s): {', '.join(missing)}"}

    df = df.dropna(subset=[base_field, compare_field])
    df = df[df[base_field].apply(lambda x: isinstance(x, (int, float)))]
    df = df[df[compare_field].apply(lambda x: isinstance(x, (int, float)))]
    if df.empty:
        return {"error": "No valid numeric data for given fields."}

    ops = {
        ">": lambda a, b: a > b,
        "<": lambda a, b: a < b,
        ">=": lambda a, b: a >= b,
        "<=": lambda a, b: a <= b,
        "==": lambda a, b: a == b,
        "!=": lambda a, b: a != b,
    }
    base_func = ops.get(base_operator)
    comp_func = ops.get(compare_operator)
    if not base_func or not comp_func:
        return {"error": "Invalid operator provided. Must be one of >, <, >=, <=, ==, !="}

    base_mask = base_func(df[base_field], base_threshold)
    filtered = df[base_mask]
    total = len(filtered)
    if total == 0:
        return {"error": f"No users match condition: {base_field} {base_operator} {base_threshold}"}

    compare_match = comp_func(filtered[compare_field], compare_threshold)
    matched = filtered[compare_match]
    match_percent = round(len(matched) / total * 100, 2)

    return {
        "base_field": base_field,
        "compare_field": compare_field,
        "base_condition": f"{base_field} {base_operator} {base_threshold}",
        "compare_condition": f"{compare_field} {compare_operator} {compare_threshold}",
        "total_base_users": int(total),
        "matching_users": int(len(matched)),
        "match_percent": match_percent,
        "visualization": "bar_chart",
        "note": "Supports all numeric operators for flexible comparative analysis."
    }


# ============================================================
# 3️⃣ CATEGORY-TO-CATEGORY ANALYSIS
# ============================================================

def conditional_category_to_category_analysis(data: list[dict], base_field: str, compare_field: str):
    """
    🧩 Category-to-Category Relationship
    ------------------------------------
    Calculates distribution of compare_field values within each base_field group.
    Example: "What % of Economy Class users are Solo Leisure travelers?"
    """

    if not isinstance(data, list) or len(data) == 0:
        return {"error": "Input must be a non-empty list of dictionaries."}

    df = pd.DataFrame(data)
    if base_field not in df.columns or compare_field not in df.columns:
        return {"error": f"Missing field(s): {base_field}, {compare_field}"}

    df = df.dropna(subset=[base_field, compare_field])
    if df.empty:
        return {"error": "No valid categorical data."}

    summary = {}
    for base_value in df[base_field].unique():
        sub = df[df[base_field] == base_value]
        total = len(sub)
        counts = sub[compare_field].value_counts(normalize=True) * 100
        counts = counts.round(2)
        if len(counts) > 10:
            top = counts.head(10)
            others = 100 - top.sum()
            counts = pd.concat([top, pd.Series({"Others": round(others, 2)})])
        summary[base_value] = {"total": total, "distribution": counts.to_dict()}

    return {
        "base_field": base_field,
        "compare_field": compare_field,
        "summary": summary,
        "visualization": "stacked_bar_chart",
        "note": "Shows compare_field distribution within each base_field category."
    }


# ============================================================
# 4️⃣ CONDITIONAL DISTRIBUTION ANALYSIS
# ============================================================

def conditional_distribution_analysis(
    data: list[dict],
    condition_field: str,
    target_field: str,
    operator: str = ">=",
    threshold: float | str | bool | None = None
):
    """
    🥧 Conditional Distribution Analysis
    ------------------------------------
    Produces a distribution of target_field filtered by condition on condition_field.
    """

    if not isinstance(data, list) or len(data) == 0:
        return {"error": "Empty or invalid data input."}

    df = pd.DataFrame(data)
    if condition_field not in df.columns or target_field not in df.columns:
        return {"error": f"Missing required field(s): {condition_field}, {target_field}"}

    df = df.dropna(subset=[condition_field, target_field])
    if df.empty:
        return {"error": "No valid data after filtering nulls."}

    ops = {
        ">": lambda a, b: a > b,
        "<": lambda a, b: a < b,
        ">=": lambda a, b: a >= b,
        "<=": lambda a, b: a <= b,
        "==": lambda a, b: a == b,
        "!=": lambda a, b: a != b,
    }
    func = ops.get(operator)
    if not func:
        return {"error": f"Invalid operator '{operator}'."}

    try:
        mask = func(df[condition_field], threshold)
    except Exception:
        df[condition_field] = df[condition_field].astype(str)
        mask = func(df[condition_field], str(threshold))

    filtered = df[mask]
    if filtered.empty:
        return {"error": f"No matches for condition {condition_field} {operator} {threshold}"}

    counts = filtered[target_field].value_counts(normalize=True) * 100
    counts = counts.round(2)
    if len(counts) > 10:
        top = counts.head(10)
        others = 100 - top.sum()
        counts = pd.concat([top, pd.Series({"Others": round(others, 2)})])

    return {
        "condition": f"{condition_field} {operator} {threshold}",
        "target_field": target_field,
        "distribution": counts.to_dict(),
        "total_filtered_rows": len(filtered),
        "visualization": "pie_chart",
        "note": "Ideal for pie or bar chart visualization of filtered distributions."
    }


# ============================================================
# 5️⃣ GENERAL PERCENTAGE DISTRIBUTION (Simplified Numeric-Only)
# ============================================================

def general_percentage_distribution(
    data: list[dict],
    field_name: str,
    operator: str,
    threshold: float
):
    """
    General Percentage Distribution (Simplified)
    --------------------------------------------
    Computes what percentage of users meet a numeric threshold
    condition such as overall_rating > 5 or wifi_connectivity >= 3.

    Parameters:
      data: list of metadata dictionaries.
      field_name: the numeric field to evaluate.
      operator: one of '>', '<', '>=', '<=', '==', '!='.
      threshold: numeric cutoff value.

    Returns a single-value result with the percentage of users
    satisfying the condition.
    """

    import pandas as pd
    import numpy as np

    if not isinstance(data, list) or len(data) == 0:
        return {"error": "Input data must be a non-empty list of dictionaries."}

    df = pd.DataFrame(data)
    if field_name not in df.columns:
        return {"error": f"Field '{field_name}' not found in dataset."}

    df = df.dropna(subset=[field_name])
    if not np.issubdtype(df[field_name].dtype, np.number):
        return {"error": f"Field '{field_name}' is not numeric."}

    ops = {
        ">": lambda a, b: a > b,
        "<": lambda a, b: a < b,
        ">=": lambda a, b: a >= b,
        "<=": lambda a, b: a <= b,
        "==": lambda a, b: a == b,
        "!=": lambda a, b: a != b,
    }

    func = ops.get(operator)
    if func is None:
        return {"error": f"Invalid operator '{operator}'."}

    total = len(df)
    match_count = func(df[field_name], threshold).sum()
    percentage = round(match_count / total * 100, 2)

    return {
        "field": field_name,
        "operator": operator,
        "threshold": threshold,
        "match_percent": percentage,
        "matching_users": int(match_count),
        "total_users": int(total),
        "visualization": "single_value",
        "note": f"{percentage}% of users have {field_name} {operator} {threshold}"
    }
