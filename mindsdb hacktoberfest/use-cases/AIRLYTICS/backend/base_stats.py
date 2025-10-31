import numpy as np
import pandas as pd
from collections import Counter
from scipy import stats

def summarize_metadata(metadata_list):
    """
    🧩 Base Statistical Summary for Airline Review Metadata
    ------------------------------------------------------
    Performs cleaning, normalization, and generates comprehensive
    statistical summaries of both numeric and categorical fields.

    Includes:
      - Numeric summaries (mean, median, std, min, max, skewness)
      - Categorical distributions (top/bottom categories)
      - Key performance metrics (recommend %, verified %, etc.)
      - Field completeness
      - Mean ratings grouped by major categories
      - Full correlation matrix (both directions)
    """

    # ------------------------------
    # Schema and Config
    # ------------------------------
    FIELD_SCHEMA = {
        "overall_rating": {"type": "numeric", "valid_range": [1, 10]},
        "seat_comfort": {"type": "numeric", "valid_range": [1, 5]},
        "cabin_staff_service": {"type": "numeric", "valid_range": [1, 5]},
        "food_beverages": {"type": "numeric", "valid_range": [1, 5]},
        "ground_service": {"type": "numeric", "valid_range": [1, 5]},
        "inflight_entertainment": {"type": "numeric", "valid_range": [1, 5]},
        "wifi_connectivity": {"type": "numeric", "valid_range": [1, 5]},
        "value_for_money": {"type": "numeric", "valid_range": [1, 5]},
        "recommended": {"type": "categorical", "values": ["yes", "no"]},
        "verified": {"type": "boolean"},
        "seat_type": {
            "type": "categorical",
            "values": ["Economy Class", "Business Class", "Premium Economy", "First Class"]
        },
        "type_of_traveller": {
            "type": "categorical",
            "values": ["Solo Leisure", "Couple Leisure", "Family Leisure", "Business"]
        },
        "airline_name": {"type": "categorical"}
    }

    # ✅ Fixed upper rating limits for display
    RATING_LIMITS = {
        "overall_rating": 10,
        "seat_comfort": 5,
        "cabin_staff_service": 5,
        "food_beverages": 5,
        "ground_service": 5,
        "inflight_entertainment": 5,
        "wifi_connectivity": 5,
        "value_for_money": 5
    }

    TOP_AIRLINES = set([
        "Frontier Airlines", "Turkish Airlines", "Thomson Airways", "China Eastern Airlines", "China Southern Airlines",
        "AirAsia India", "Vietnam Airlines", "Air Serbia", "FlySafair", "Air India",
        "Norwegian", "United Airlines", "Oman Air", "Breeze Airways", "Transavia",
        "Singapore Airlines", "Air New Zealand", "PLAY", "Garuda Indonesia", "Air Berlin",
        "Iberia", "Finnair", "Royal Brunei Airlines", "Go First", "Virgin America",
        "CSA Czech Airlines", "Etihad Airways", "Korean Air", "Hawaiian Airlines", "Egyptair",
        "El Al Israel Airlines", "Hong Kong Airlines", "Thomas Cook Airlines", "easyJet", "Gulf Air",
        "Qatar Airways", "Air France", "Nok Air", "Thai Smile Airways", "Porter Airlines",
        "Virgin Australia", "Malindo Air", "Emirates", "Air Mauritius", "Hainan Airlines",
        "Jetstar Asia", "Delta Air Lines", "Tigerair", "Kuwait Airways", "Air Canada"
    ])

    # ------------------------------
    # Validate Input
    # ------------------------------
    if not metadata_list or not isinstance(metadata_list, list):
        return {"error": "Input must be a list of metadata dictionaries."}

    cleaned = []
    for m in metadata_list:
        if not isinstance(m, dict):
            continue
        m = m.copy()

        # Group rare airlines
        airline = m.get("airline_name")
        if airline and airline not in TOP_AIRLINES:
            m["airline_name"] = "Others"

        # Clean numeric ranges
        for field, info in FIELD_SCHEMA.items():
            if info["type"] == "numeric":
                val = m.get(field)
                if not isinstance(val, (int, float)):
                    m[field] = None
                elif val < info["valid_range"][0] or val > info["valid_range"][1]:
                    m[field] = None
        cleaned.append(m)

    if not cleaned:
        return {"error": "No valid metadata found after cleaning."}

    df = pd.DataFrame(cleaned)
    total = len(df)
    stats_dict = {"total_rows": total}

    # ------------------------------
    # Numeric Field Statistics
    # ------------------------------
    numeric_stats = {}
    for field, info in FIELD_SCHEMA.items():
        if info["type"] != "numeric" or field not in df:
            continue
        values = df[field].dropna().astype(float)
        if values.empty:
            continue

        q1, q3 = np.percentile(values, [25, 75])
        iqr = q3 - q1
        outliers = ((values < (q1 - 1.5 * iqr)) | (values > (q3 + 1.5 * iqr))).sum()

        # ✅ Always use predefined rating limit if available
        max_limit = RATING_LIMITS.get(field, float(values.max()))

        numeric_stats[field] = {
            "mean": round(values.mean(), 2),
            "median": round(values.median(), 2),
            "std": round(values.std(), 2),
            "min": float(values.min()),
            "max": float(max_limit),
            "skew": round(stats.skew(values), 2),
            "outliers": int(outliers),
            "count": int(values.count())
        }

    stats_dict["numeric"] = numeric_stats

    # ------------------------------
    # Categorical Field Statistics
    # ------------------------------
    categorical_stats = {}
    for field, info in FIELD_SCHEMA.items():
        if info["type"] not in ["categorical", "boolean"] or field not in df:
            continue
        values = df[field].dropna().astype(str)
        if values.empty:
            continue

        counts = values.value_counts(normalize=True) * 100
        counts = counts.round(2).to_dict()

        top3 = dict(sorted(counts.items(), key=lambda x: -x[1])[:3])
        bottom3 = dict(sorted(counts.items(), key=lambda x: x[1])[:3])

        categorical_stats[field] = {
            "unique_values": len(counts),
            "distribution": counts,
            "top3": top3,
            "bottom3": bottom3
        }

    stats_dict["categorical"] = categorical_stats

    # ------------------------------
    # Key Metrics
    # ------------------------------
    pos_reviews = df["overall_rating"].dropna()
    pos_review_pct = round((pos_reviews >= 5).mean() * 100, 2) if not pos_reviews.empty else None

    stats_dict["key_metrics"] = {
        "positive_review_pct": pos_review_pct,
        "recommended_yes_pct": round((df["recommended"] == "yes").mean() * 100, 2) if "recommended" in df else None,
        "verified_user_pct": round((df["verified"] == True).mean() * 100, 2) if "verified" in df else None
    }

    # ------------------------------
    # Field Completeness
    # ------------------------------
    completeness = {col: round(df[col].notna().mean() * 100, 2) for col in df.columns}
    stats_dict["field_completeness_pct"] = completeness

    # ------------------------------
    # Average Ratings by Category
    # ------------------------------
    group_avgs = {}
    for cat_field in ["airline_name", "seat_type", "type_of_traveller"]:
        if cat_field in df and "overall_rating" in df:
            temp = df[[cat_field, "overall_rating"]].dropna()
            if not temp.empty:
                group_avgs[cat_field] = temp.groupby(cat_field)["overall_rating"].mean().round(2).to_dict()

    stats_dict["avg_rating_by_category"] = group_avgs

    # ------------------------------
    # Correlation Matrix
    # ------------------------------
    numeric_cols = [f for f, info in FIELD_SCHEMA.items() if info["type"] == "numeric" and f in df]
    corr_matrix = df[numeric_cols].corr(method="pearson").round(3).fillna(0).to_dict()

    stats_dict["correlations"] = corr_matrix

    # ------------------------------
    # Final Output
    # ------------------------------
    return {
        "summary": stats_dict,
        "note": "Cleaned invalid values (-1), grouped rare airlines into 'Others', fixed rating scales enforced (/5 or /10)."
    }

# Example Test
if __name__ == "__main__":
    sample = [
        {
            "airline_name": "Emirates",
            "overall_rating": 8,
            "seat_comfort": 5,
            "wifi_connectivity": 3,
            "recommended": "yes",
            "verified": True,
            "seat_type": "Economy Class",
            "type_of_traveller": "Couple Leisure"
        },
        {
            "airline_name": "Air India",
            "overall_rating": 2,
            "seat_comfort": 1,
            "wifi_connectivity": 4,
            "recommended": "no",
            "verified": True,
            "seat_type": "Business Class",
            "type_of_traveller": "Solo Leisure"
        }
    ]
    import json
    print(json.dumps(summarize_metadata(sample), indent=2))
