import mindsdb_sdk


def add_refresh_kb_jobs():
    server = mindsdb_sdk.connect()

    jira_job_query = """
    CREATE JOB jira_refresh_job (
        insert into jira_kb
        select * from jira_datasource.issues;
    ) EVERY 30 minutes;
    """

    zendesk_job_query = """
    CREATE JOB zendesk_refresh_job (
        insert into zendesk_kb
        select * from zendesk_datasource.tickets;
    ) EVERY 30 minutes;
    """

    confluence_job_query = """
    CREATE JOB confluence_refresh_job (
        insert into confluence_kb
        select * from confluence_datasource.pages;
    ) EVERY 1 day;
    """

    try:
        server.query(jira_job_query).fetch()
    except Exception as e:
        print(f"jira KB refresh job exception {e}")
    try:
        server.query(zendesk_job_query).fetch()
    except Exception as e:
        print(f"zendesk KB refresh job exception {e}")
    try:
        server.query(confluence_job_query).fetch()
    except Exception as e:
        print(f"confluence KB refresh job exception {e}")

    print("Jobs to create KBs are created")


if __name__ == "__main__":
    add_refresh_kb_jobs()
