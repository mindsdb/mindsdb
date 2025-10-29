"""MindsDB JOB management for automated analytics processing.

This module creates scheduled JOBs that run periodically to:
1. Analyze daily conversation data
2. Generate weekly trend reports
"""

from typing import Optional


def check_job_exists(server, job_name: str) -> bool:
    """Check if a JOB exists in MindsDB.

    Args:
        server: MindsDB server connection
        job_name: Name of the JOB to check

    Returns:
        True if JOB exists, False otherwise
    """
    try:
        result = server.query("SHOW JOBS")
        data = result.fetch()
        return job_name in data["NAME"].values if "NAME" in data.columns else False
    except Exception as e:
        print(f"⚠ Error checking JOB '{job_name}': {e}")
        return False


def drop_job_if_exists(server, job_name: str) -> bool:
    """Drop a JOB if it exists (for recreating with updated logic).

    Args:
        server: MindsDB server connection
        job_name: Name of the JOB to drop

    Returns:
        True if operation was successful
    """
    try:
        if check_job_exists(server, job_name):
            server.query(f"DROP JOB {job_name}")
            print(f"✓ Dropped existing JOB '{job_name}'")
        return True
    except Exception as e:
        print(f"✗ Error dropping JOB '{job_name}': {e}")
        return False


def create_daily_analytics_job(server, recreate: bool = False) -> bool:
    """Create JOB for daily conversation analysis.

    This JOB runs daily at 23:00 to analyze all conversations from the current day.
    It uses the analytics_agent to identify top issues, calculate resolution rates,
    and generate insights.

    Args:
        server: MindsDB server connection
        recreate: If True, drop existing JOB and recreate it

    Returns:
        True if JOB was created successfully
    """
    job_name = "daily_conversation_analysis"

    if recreate:
        drop_job_if_exists(server, job_name)
    elif check_job_exists(server, job_name):
        print(f"✓ JOB '{job_name}' already exists, skipping creation")
        return True

    print(f"\nCreating JOB '{job_name}'...")

    # MindsDB JOB query - runs daily at 23:00
    query = """
        INSERT INTO banking_postgres_db.daily_analytics (
            analysis_date,
            total_conversations,
            resolved_count,
            unresolved_count,
            resolution_rate,
            top_issues,
            key_insights,
            recommendations
        )
        SELECT
            CURRENT_DATE as analysis_date,
            COUNT(*) as total_conversations,
            SUM(CASE WHEN resolved = TRUE THEN 1 ELSE 0 END) as resolved_count,
            SUM(CASE WHEN resolved = FALSE THEN 1 ELSE 0 END) as unresolved_count,
            ROUND(
                100.0 * SUM(CASE WHEN resolved = TRUE THEN 1 ELSE 0 END) / COUNT(*),
                2
            ) as resolution_rate,
            (
                SELECT answer->>'top_issues'
                FROM analytics_agent
                WHERE question = CONCAT(
                    'Total conversations: ', CAST(COUNT(*) AS TEXT),
                    ', Resolved: ', CAST(SUM(CASE WHEN resolved = TRUE THEN 1 ELSE 0 END) AS TEXT),
                    ', Unresolved: ', CAST(SUM(CASE WHEN resolved = FALSE THEN 1 ELSE 0 END) AS TEXT),
                    ', Summaries: ',
                    STRING_AGG(summary, ' | ')
                )
                LIMIT 1
            )::jsonb as top_issues,
            (
                SELECT answer->>'key_insights'
                FROM analytics_agent
                WHERE question = CONCAT(
                    'Total conversations: ', CAST(COUNT(*) AS TEXT),
                    ', Resolved: ', CAST(SUM(CASE WHEN resolved = TRUE THEN 1 ELSE 0 END) AS TEXT),
                    ', Unresolved: ', CAST(SUM(CASE WHEN resolved = FALSE THEN 1 ELSE 0 END) AS TEXT),
                    ', Summaries: ',
                    STRING_AGG(summary, ' | ')
                )
                LIMIT 1
            ) as key_insights,
            (
                SELECT answer->>'recommendations'
                FROM analytics_agent
                WHERE question = CONCAT(
                    'Total conversations: ', CAST(COUNT(*) AS TEXT),
                    ', Resolved: ', CAST(SUM(CASE WHEN resolved = TRUE THEN 1 ELSE 0 END) AS TEXT),
                    ', Unresolved: ', CAST(SUM(CASE WHEN resolved = FALSE THEN 1 ELSE 0 END) AS TEXT),
                    ', Summaries: ',
                    STRING_AGG(summary, ' | ')
                )
                LIMIT 1
            ) as recommendations
        FROM banking_postgres_db.conversations_summary
        WHERE DATE(created_at) = CURRENT_DATE
        AND summary IS NOT NULL
        GROUP BY DATE(created_at)
    """

    try:
        # Use SDK API instead of SQL CREATE JOB
        server.create_job(
            name=job_name,
            query_str=query,
            repeat_str='1 day'
        )
        print(f"✓ Successfully created JOB '{job_name}'")
        print(f"  Schedule: Daily at 23:00")
        return True
    except Exception as e:
        print(f"✗ Failed to create JOB '{job_name}': {e}")
        return False


def create_weekly_trends_job(server, recreate: bool = False) -> bool:
    """Create JOB for weekly trend analysis.

    This JOB runs weekly on Sunday at 23:30 to analyze the past week's data,
    identify trends, and generate strategic recommendations.

    Args:
        server: MindsDB server connection
        recreate: If True, drop existing JOB and recreate it

    Returns:
        True if JOB was created successfully
    """
    job_name = "weekly_trends_analysis"

    if recreate:
        drop_job_if_exists(server, job_name)
    elif check_job_exists(server, job_name):
        print(f"✓ JOB '{job_name}' already exists, skipping creation")
        return True

    print(f"\nCreating JOB '{job_name}'...")

    # MindsDB JOB query - runs weekly on Sunday at 23:30
    query = """
        INSERT INTO banking_postgres_db.weekly_trends (
            week_start_date,
            week_end_date,
            total_conversations,
            avg_resolution_rate,
            trend_direction,
            trending_issues,
            emerging_patterns,
            trend_summary,
            strategic_recommendations
        )
        SELECT
            DATE_TRUNC('week', CURRENT_DATE)::date as week_start_date,
            CURRENT_DATE as week_end_date,
            SUM(total_conversations) as total_conversations,
            ROUND(AVG(resolution_rate), 2) as avg_resolution_rate,
            CASE
                WHEN AVG(resolution_rate) > (
                    SELECT AVG(resolution_rate)
                    FROM banking_postgres_db.daily_analytics
                    WHERE analysis_date >= CURRENT_DATE - INTERVAL '14 days'
                    AND analysis_date < CURRENT_DATE - INTERVAL '7 days'
                ) THEN 'improving'
                WHEN AVG(resolution_rate) < (
                    SELECT AVG(resolution_rate)
                    FROM banking_postgres_db.daily_analytics
                    WHERE analysis_date >= CURRENT_DATE - INTERVAL '14 days'
                    AND analysis_date < CURRENT_DATE - INTERVAL '7 days'
                ) THEN 'declining'
                ELSE 'stable'
            END as trend_direction,
            (
                SELECT answer->>'trending_issues'
                FROM analytics_agent
                WHERE question = CONCAT(
                    'Weekly Summary - Total: ', CAST(SUM(total_conversations) AS TEXT),
                    ', Avg Resolution Rate: ', CAST(ROUND(AVG(resolution_rate), 2) AS TEXT),
                    ', Daily Data: ',
                    STRING_AGG(
                        CONCAT('Date: ', analysis_date, ' - ', key_insights),
                        ' | '
                    )
                )
                LIMIT 1
            )::jsonb as trending_issues,
            (
                SELECT answer->>'emerging_patterns'
                FROM analytics_agent
                WHERE question = CONCAT(
                    'Weekly Summary - Total: ', CAST(SUM(total_conversations) AS TEXT),
                    ', Avg Resolution Rate: ', CAST(ROUND(AVG(resolution_rate), 2) AS TEXT),
                    ', Daily Data: ',
                    STRING_AGG(
                        CONCAT('Date: ', analysis_date, ' - ', key_insights),
                        ' | '
                    )
                )
                LIMIT 1
            )::jsonb as emerging_patterns,
            (
                SELECT answer->>'trend_summary'
                FROM analytics_agent
                WHERE question = CONCAT(
                    'Weekly Summary - Total: ', CAST(SUM(total_conversations) AS TEXT),
                    ', Avg Resolution Rate: ', CAST(ROUND(AVG(resolution_rate), 2) AS TEXT),
                    ', Daily Data: ',
                    STRING_AGG(
                        CONCAT('Date: ', analysis_date, ' - ', key_insights),
                        ' | '
                    )
                )
                LIMIT 1
            ) as trend_summary,
            (
                SELECT answer->>'strategic_recommendations'
                FROM analytics_agent
                WHERE question = CONCAT(
                    'Weekly Summary - Total: ', CAST(SUM(total_conversations) AS TEXT),
                    ', Avg Resolution Rate: ', CAST(ROUND(AVG(resolution_rate), 2) AS TEXT),
                    ', Daily Data: ',
                    STRING_AGG(
                        CONCAT('Date: ', analysis_date, ' - ', key_insights),
                        ' | '
                    )
                )
                LIMIT 1
            ) as strategic_recommendations
        FROM banking_postgres_db.daily_analytics
        WHERE analysis_date >= DATE_TRUNC('week', CURRENT_DATE)
        AND analysis_date <= CURRENT_DATE
        GROUP BY DATE_TRUNC('week', CURRENT_DATE)
    """

    try:
        # Use SDK API instead of SQL CREATE JOB
        server.create_job(
            name=job_name,
            query_str=query,
            repeat_str='1 week'
        )
        print(f"✓ Successfully created JOB '{job_name}'")
        print(f"  Schedule: Weekly on Sunday at 23:30")
        return True
    except Exception as e:
        print(f"✗ Failed to create JOB '{job_name}': {e}")
        return False


def init_mindsdb_jobs(server, recreate: bool = False, verbose: bool = True) -> bool:
    """Initialize all MindsDB JOBs for analytics automation.

    Args:
        server: MindsDB server connection
        recreate: If True, drop and recreate all JOBs
        verbose: Print progress messages

    Returns:
        True if all JOBs created successfully
    """
    if verbose:
        print("=" * 70)
        print("Initializing MindsDB Analytics JOBs")
        print("=" * 70)

    success = True

    # Step 1: Create daily analytics JOB
    if verbose:
        print("\n[1/2] Setting up daily conversation analysis...")
    if not create_daily_analytics_job(server, recreate=recreate):
        if verbose:
            print("⚠ Daily analytics JOB creation had issues")
        success = False

    # Step 2: Create weekly trends JOB
    if verbose:
        print("\n[2/2] Setting up weekly trend analysis...")
    if not create_weekly_trends_job(server, recreate=recreate):
        if verbose:
            print("⚠ Weekly trends JOB creation had issues")
        success = False

    if verbose:
        print("\n" + "=" * 70)
        if success:
            print("MindsDB JOBs Initialization Complete!")
        else:
            print("MindsDB JOBs Initialization Completed with Warnings")
        print("=" * 70)
        print("\nScheduled JOBs:")
        print("  • daily_conversation_analysis - Daily at 23:00")
        print("  • weekly_trends_analysis - Sunday at 23:30")

    return success


if __name__ == "__main__":
    # Can be run standalone for testing
    import mindsdb_sdk
    import os
    from dotenv import load_dotenv

    load_dotenv()
    MINDSDB_URL = os.getenv("MINDSDB_URL", "http://127.0.0.1:47334")

    server = mindsdb_sdk.connect(MINDSDB_URL)
    init_mindsdb_jobs(server, recreate=False, verbose=True)
