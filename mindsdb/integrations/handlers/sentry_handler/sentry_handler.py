from mindsdb.integrations.handlers.sentry_handler.explore.handler import ExploreSentryHandler
from mindsdb.integrations.handlers.sentry_handler.issue.handler import IssueSentryHandler


class SentryHandler(IssueSentryHandler):
    """Compatibility entrypoint kept for the public sentry handler package."""

    def __init__(self, name: str, connection_data: dict, **kwargs) -> None:
        super().__init__(name, connection_data, **kwargs)
        self.explore_handler = ExploreSentryHandler(name, self.connection_data, issue_handler=self)
        for table_name, table in self.explore_handler._tables.items():
            self._register_table(table_name, table)
