from ..postgres_handler import Handler as PostgresHandler


class SupabaseHandler(PostgresHandler):
    """
    This handler handles connection and execution of the Supabase statements.
    """
    name = 'supabase'

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
