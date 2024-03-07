import json
import traceback
from typing import Dict, List
from pathlib import Path
from http import HTTPStatus

from flask import request
from flask_restx import Resource

from mindsdb.interfaces.storage.fs import FileStorageFactory, RESOURCE_GROUP
from mindsdb.api.http.namespaces.configs.tabs import ns_conf
from mindsdb.utilities import log
from mindsdb.utilities.context import context as ctx
from mindsdb.api.http.utils import http_error
from mindsdb.utilities.exception import EntityNotExistsError


logger = log.getLogger(__name__)


TABS_FILENAME = 'tabs'


def get_storage():

    storageFactory = FileStorageFactory(
        resource_group=RESOURCE_GROUP.TAB,
        sync=True
    )

    # resource_id is useless for 'tabs'
    # use constant
    return storageFactory(0)


# def _get_request_body():
#     """
#     """
#     try:
#         data = request.json
#     except Exception:
#         return http_error(400, 'Error', 'Request body must be json')
#     if (
#         isinstance(data, dict) is False
#         or len(data.keys()) == 0
#         or len(set(data.keys()) - {'index', 'name', 'content'}) != 0
#     ):
#         return http_error(400, 'Error', 'Invalid parameters for adding tab')
#     return data


def _is_requies_valid():
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


class TabsController:
    def __init__(self):
        self.storage_factory = FileStorageFactory(
            resource_group=RESOURCE_GROUP.TAB,
            sync=True
        )
        self.file_storage = self.storage_factory(0)

    def _get_next_tab_id(self) -> int:
        """
        """
        tabs_files = self._get_tabs_files()
        tabs_ids = list(tabs_files.keys())
        if len(tabs_ids) == 0:
            return 1
        return max(tabs_ids) + 1

    def _get_tabs_files(self) -> Dict[int, Path]:
        """
        """
        tabs = {}
        for child in self.file_storage.folder_path.iterdir():
            if (child.is_file() and child.name.startswith('tab_')) is False:
                continue
            tab_id = child.name.replace('tab_', '')
            if tab_id.isnumeric() is False:
                continue
            tabs[int(tab_id)] = child
        return tabs

    def _migrate_legacy(self) -> None:
        """convert old single-file tabs storage to multiple files
        """
        file_storage = self.file_storage
        try:
            file_data = file_storage.file_get(TABS_FILENAME)
        except FileNotFoundError:
            return
        except Exception:
            file_storage.delete()
            return

        try:
            data = json.loads(file_data)
        except Exception:
            file_storage.delete()
            return

        if isinstance(data, dict) is False or isinstance(data.get('tabs'), str) is False:
            file_storage.delete()
            return

        try:
            tabs_list = json.loads(data['tabs'])
        except Exception:
            file_storage.delete()
            return

        if isinstance(tabs_list, list) is False:
            file_storage.delete()
            return

        for tab in tabs_list:
            tab_id = self._get_next_tab_id()

            b_types = json.dumps({
                'index': tab.get('index', 0),
                'name': tab.get('name', 'undefined'),
                'content': tab.get('value', '')
            }).encode("utf-8")
            file_storage.file_set(f'tab_{tab_id}', b_types)

        file_storage.delete(TABS_FILENAME)

    def get_all(self) -> List[Dict]:
        """
        """
        self.file_storage.pull()
        self._migrate_legacy()

        tabs_files = self._get_tabs_files()
        tabs_list = []
        for tab_id, tab_path in tabs_files.items():
            try:
                data = json.loads(tab_path.read_text())
            except Exception as e:
                logger.error(f"Can't read data of tab {ctx.company_id}/{tab_id}: {e}")
                continue
            tabs_list.append({
                'id': tab_id,
                **data
            })

        return tabs_list

    def get(self, tab_id: int) -> Dict:
        """
        """
        if isinstance(tab_id, int) is False:
            raise ValueError('Tab id must be integer')

        try:
            raw_tab_data = self.file_storage.file_get(f'tab_{tab_id}')
        except FileNotFoundError:
            raise EntityNotExistsError(f'tab {tab_id}')

        # tab_path = self.file_storage.folder_path / f'tab_{tab_id}'
        # if tab_path.is_file() is False:
        #     raise EntityNotExistsError(f'tab {tab_id}')

        try:
            data = json.loads(raw_tab_data)
        except Exception as e:
            logger.error(f"Can't read data of tab {ctx.company_id}/{tab_id}: {e}")
            raise Exception(f"Can't read data of tab: {e}")

        return {
            'id': tab_id,
            **data
        }

    def add(self, index: int = 0, name: str = 'undefined', content: str = '') -> int:
        tab_id = self._get_next_tab_id()

        data_bytes = json.dumps({
            'index': index,
            'name': name,
            'content': content
        }).encode("utf-8")
        self.file_storage.file_set(f'tab_{tab_id}', data_bytes)

        return tab_id

    def modify(self, tab_id: int, index: int = None, name: str = None, content: str = None):
        current_data = self.get(tab_id)

        # region modify index
        if index is not None and current_data['index'] != index:
            current_data['index'] = index
            all_tabs = [x for x in self.get_all() if x['id'] != tab_id]
            all_tabs.sort(key=lambda x: x['index'])
            self.file_storage.sync = False
            for tab_index, tab in enumerate(all_tabs):
                if tab_index < index:
                    tab['index'] = tab_index
                else:
                    tab['index'] = tab_index + 1
                data_bytes = json.dumps(tab).encode('utf-8')
                self.file_storage.file_set(f'tab_{tab["id"]}', data_bytes)
            self.file_storage.sync = True
            self.file_storage.push()
        # endregion

        # region modify name
        if name is not None and current_data['name'] != name:
            current_data['name'] = name
        # endregion

        # region modify content
        if content is not None and current_data['content'] != content:
            current_data['content'] = content
        # endregion

        data_bytes = json.dumps(current_data).encode('utf-8')
        self.file_storage.file_set(f'tab_{tab_id}', data_bytes)

    def delete(self, tab_id: int):
        try:
            self.file_storage.file_get(f'tab_{tab_id}')
        except FileNotFoundError:
            raise EntityNotExistsError(f'tab {tab_id}')

        self.file_storage.delete(f'tab_{tab_id}')


tabs_controller = TabsController()


@ns_conf.route('/')
class Tabs(Resource):
    @ns_conf.doc('get_tabs')
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

    @ns_conf.doc('save_tabs')
    def post(self):
        mode = request.args.get('mode')

        if mode == 'new':
            if _is_requies_valid() is False:
                return http_error(400, 'Error', 'Invalid parameters')
            data = request.json
            new_tab_id = tabs_controller.add(**data)
            return new_tab_id, 200
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
@ns_conf.param("tab_id", "id for tab")
class Tab(Resource):
    @ns_conf.doc("put_tab")
    def put(self, tab_id: int):
        if _is_requies_valid() is False:
            return http_error(400, 'Error', 'Invalid parameters')
        data = request.json
        tabs_controller.modify(int(tab_id), **data)
        return '', 200

    @ns_conf.doc("delete_tab")
    def delete(self, tab_id: int):
        try:
            tabs_controller.delete(int(tab_id))
        except EntityNotExistsError:
            return http_error(400, 'Error', 'Tab not exists')
        return '', 200
