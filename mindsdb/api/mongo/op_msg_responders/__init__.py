from .whatsmyuri import responder as responder_whatsmyuri
from .buildinfo import responder as responder_buildinfo
from .is_master import responder as responder_is_master
from .replsetgetstatus import responder as responder_replsetgetstatus
from .getlog import responder as responder_getlog

from .list_collections import responder as responder_list_collections
from .list_databases import responder as responder_list_databases

from .find import responder as responder_find
from .insert import responder as responder_insert

responders = [
    # service queries
    responder_whatsmyuri,
    responder_buildinfo,
    # responder_is_master,
    responder_replsetgetstatus,
    responder_getlog,
    #
    responder_list_collections,
    responder_list_databases,
    responder_find,
    responder_insert
]
