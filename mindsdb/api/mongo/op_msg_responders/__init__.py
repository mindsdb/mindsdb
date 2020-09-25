from .whatsmyuri import responder as responder_whatsmyuri
from .buildinfo import responder as responder_buildinfo
from .is_master import responder as responder_is_master
from .is_master_lower import responder as responder_is_master_lower
from .replsetgetstatus import responder as responder_replsetgetstatus
from .getlog import responder as responder_getlog
from .add_shard import responder as responder_add_shard
from .update_range_deletions import responder as responder_update_range_deletions
from .recv_chunk_start import responder as responder_recv_chunk_start

from .list_collections import responder as responder_list_collections
from .list_databases import responder as responder_list_databases

from .find import responder as responder_find
from .insert import responder as responder_insert
from .delete import responder as responder_delete

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
    # user queries
    responder_list_collections,
    responder_list_databases,
    responder_find,
    responder_insert,
    responder_delete
]
