from .whatsmyuri import responder as responder_whatsmyuri
from .buildinfo import responder as responder_buildinfo
from .is_master import responder as responder_is_master
from .is_master_lower import responder as responder_is_master_lower
from .replsetgetstatus import responder as responder_replsetgetstatus
from .getlog import responder as responder_getlog
from .add_shard import responder as responder_add_shard
from .update_range_deletions import responder as responder_update_range_deletions
from .recv_chunk_start import responder as responder_recv_chunk_start
from .connection_status import responder as responder_connection_status
from .get_cmd_line_opts import responder as responder_get_cmd_line_opts
from .host_info import responder as responder_host_info
from .db_stats import responder as responder_db_stats
from .coll_stats import responder as responder_coll_stats
from .count import responder as responder_count
from .aggregate import responder as responder_aggregate

from .list_indexes import responder as responder_list_indexes
from .list_collections import responder as responder_list_collections
from .list_databases import responder as responder_list_databases

from .find import responder as responder_find
from .insert import responder as responder_insert
from .delete import responder as responder_delete

from .sasl_start import responder as sasl_start
from .sasl_continue import responder as sasl_continue

responders = [
    # service queries
    responder_whatsmyuri,
    responder_buildinfo,
    responder_is_master,
    responder_is_master_lower,
    responder_replsetgetstatus,
    responder_getlog,
    responder_add_shard,                # 4.4
    responder_update_range_deletions,   # 4.4
    responder_recv_chunk_start,         # 4.4
    responder_connection_status,
    responder_get_cmd_line_opts,
    responder_host_info,
    responder_db_stats,
    responder_coll_stats,
    responder_count,
    responder_aggregate,
    # user queries
    responder_list_indexes,
    responder_list_collections,
    responder_list_databases,
    responder_find,
    responder_insert,
    responder_delete,
    # auth
    sasl_start,
    sasl_continue
]
