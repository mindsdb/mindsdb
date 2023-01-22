import json
from flask import request
from flask_restx import Resource

from mindsdb.interfaces.storage.fs import FileStorageFactory, RESOURCE_GROUP
from mindsdb.api.http.namespaces.configs.tabs import ns_conf
from mindsdb.utilities.log import get_log

logger = get_log("main")


TABS_FILENAME = 'tabs'


def get_storage():

    storageFactory = FileStorageFactory(
        resource_group=RESOURCE_GROUP.TAB,
        sync=True
    )

    # resource_id is useless for 'tabs'
    # use constant
    return storageFactory(0)


@ns_conf.route('/')
class Tab(Resource):
    @ns_conf.doc('get_tabs')
    def get(self):
        storage = get_storage()
        tabs = None
        try:
            raw_data = storage.file_get(TABS_FILENAME)
            tabs = json.loads(raw_data)
        except Exception as e:
            logger.warning("unable to get tabs data - %s", e)
            return {}, 200
        return tabs, 200

    @ns_conf.doc('save_tabs')
    def post(self):
        storage = get_storage()
        try:
            tabs = request.json
            b_types = json.dumps(tabs).encode("utf-8")
            storage.file_set(TABS_FILENAME, b_types)
        except Exception as e:
            logger.warning("unable to store tabs data - %s", e)
            return str(e), 500

        return '', 200
