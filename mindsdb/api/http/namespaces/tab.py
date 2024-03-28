import json
import traceback
from http import HTTPStatus

from flask import request
from flask_restx import Resource

from mindsdb.metrics.metrics import api_endpoint_metrics
from mindsdb.api.http.namespaces.configs.tabs import ns_conf
from mindsdb.utilities import log
from mindsdb.api.http.utils import http_error
from mindsdb.utilities.exception import EntityNotExistsError
from mindsdb.interfaces.tabs.tabs_controller import tabs_controller, get_storage, TABS_FILENAME


logger = log.getLogger(__name__)


def _is_request_valid() -> bool:
    """check if request body contains all (and only) required fields

    Returns:
        bool: True if all required data in the request
    """
    try:
        data = request.json
    except Exception:
        return False
    if (
        isinstance(data, dict) is False
        or len(data.keys()) == 0
        or len(set(data.keys()) - {'index', 'name', 'content'}) != 0
    ):
        return False
    return True


@ns_conf.route('/')
class Tabs(Resource):
    @ns_conf.doc('get_tabs')
    @api_endpoint_metrics('GET', '/tabs')
    def get(self):
        mode = request.args.get('mode')

        if mode == 'new':
            return tabs_controller.get_all(), 200
        else:
            # deprecated
            storage = get_storage()
            tabs = None
            try:
                raw_data = storage.file_get(TABS_FILENAME)
                tabs = json.loads(raw_data)
            except Exception as e:
                logger.warning("unable to get tabs data - %s", e)
                return {}, 200
            return tabs, 200

    @ns_conf.doc('save_tab')
    @api_endpoint_metrics('POST', '/tabs')
    def post(self):
        mode = request.args.get('mode')

        if mode == 'new':
            if _is_request_valid() is False:
                return http_error(400, 'Error', 'Invalid parameters')
            data = request.json
            tab_meta = tabs_controller.add(**data)
            tabs_meta = tabs_controller._get_tabs_meta()
            return {
                'tab_meta': tab_meta,
                'tabs_meta': tabs_meta
            }, 200
        else:
            # deprecated
            storage = get_storage()
            try:
                tabs = request.json
                b_types = json.dumps(tabs).encode("utf-8")
                storage.file_set(TABS_FILENAME, b_types)
            except Exception as e:
                logger.error("unable to store tabs data - %s", e)
                logger.error(traceback.format_exc())
                return http_error(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    "Can't save tabs",
                    'something went wrong during tabs saving'
                )

            return '', 200


@ns_conf.route("/<tab_id>")
@ns_conf.param("tab_id", "id of tab")
class Tab(Resource):
    @ns_conf.doc("get_tab")
    @api_endpoint_metrics('GET', '/tabs/tab')
    def get(self, tab_id: int):
        try:
            tab_data = tabs_controller.get(int(tab_id))
        except EntityNotExistsError:
            return http_error(404, 'Error', 'The tab does not exist')

        return tab_data, 200

    @ns_conf.doc("put_tab")
    @api_endpoint_metrics('PUT', '/tabs/tab')
    def put(self, tab_id: int):
        if _is_request_valid() is False:
            return http_error(400, 'Error', 'Invalid parameters')
        data = request.json
        try:
            tab_meta = tabs_controller.modify(int(tab_id), **data)
        except EntityNotExistsError:
            return http_error(404, 'Error', 'The tab does not exist')

        tabs_meta = tabs_controller._get_tabs_meta()

        return {
            'tab_meta': tab_meta,
            'tabs_meta': tabs_meta
        }, 200

    @ns_conf.doc("delete_tab")
    @api_endpoint_metrics('DELETE', '/tabs/tab')
    def delete(self, tab_id: int):
        try:
            tabs_controller.delete(int(tab_id))
        except EntityNotExistsError:
            return http_error(404, 'Error', 'The tab does not exist')
        return '', 200
