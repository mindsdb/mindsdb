import json
from typing import Dict, List
from pathlib import Path

from mindsdb.utilities import log
from mindsdb.utilities.context import context as ctx
from mindsdb.utilities.exception import EntityNotExistsError
from mindsdb.interfaces.storage.fs import FileStorageFactory, RESOURCE_GROUP, FileStorage


logger = log.getLogger(__name__)


TABS_FILENAME = 'tabs'


def get_storage():
    # deprecated

    storageFactory = FileStorageFactory(
        resource_group=RESOURCE_GROUP.TAB,
        sync=True
    )

    # resource_id is useless for 'tabs'
    # use constant
    return storageFactory(0)


class TabsController:
    """Tool for adding, editing, and deleting user's tabs

    Attributes:
        storage_factory (FileStorageFactory): callable object which returns tabs file storage
    """

    def __init__(self) -> None:
        self.storage_factory = FileStorageFactory(
            resource_group=RESOURCE_GROUP.TAB,
            sync=True
        )

    def _get_file_storage(self) -> FileStorage:
        """Get user's tabs file storage
           NOTE: file storage depend is company_id sensitive, so need to recreate it each time

        Returns:
            FileStorage
        """
        return self.storage_factory(0)

    def _get_next_tab_id(self) -> int:
        """Get next free tab id

        Returns:
            int: id for next tab
        """
        tabs_files = self._get_tabs_files()
        tabs_ids = list(tabs_files.keys())
        if len(tabs_ids) == 0:
            return 1
        return max(tabs_ids) + 1

    def _get_tabs_files(self) -> Dict[int, Path]:
        """Get list of paths to each tab file

        Returns:
            Dict[int, Path]
        """
        tabs = {}
        for child in self._get_file_storage().folder_path.iterdir():
            if (child.is_file() and child.name.startswith('tab_')) is False:
                continue
            tab_id = child.name.replace('tab_', '')
            if tab_id.isnumeric() is False:
                continue
            tabs[int(tab_id)] = child
        return tabs

    def _get_tabs_meta(self) -> List[Dict]:
        """Get tabs info without content

        Returns:
            List[Dict]
        """
        all_tabs = self.get_all()
        for tab in all_tabs:
            del tab['content']
        return all_tabs

    def _migrate_legacy(self) -> None:
        """Convert old single-file tabs storage to multiple files
        """
        file_storage = self._get_file_storage()
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
        """Get list of all tabs

        Returns:
            List[Dict]: all tabs data
        """
        self._get_file_storage().pull()
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

        tabs_list.sort(key=lambda x: x['index'])
        return tabs_list

    def get(self, tab_id: int) -> Dict:
        """Get data of single tab

        Args:
            tab_id (int): id of the tab

        Returns:
            dict: tabs data
        """
        if isinstance(tab_id, int) is False:
            raise ValueError('Tab id must be integer')

        try:
            raw_tab_data = self._get_file_storage().file_get(f'tab_{tab_id}')
        except FileNotFoundError:
            raise EntityNotExistsError(f'tab {tab_id}')

        try:
            data = json.loads(raw_tab_data)
        except Exception as e:
            logger.error(f"Can't read data of tab {ctx.company_id}/{tab_id}: {e}")
            raise Exception(f"Can't read data of tab: {e}")

        return {
            'id': tab_id,
            **data
        }

    def add(self, index: int = None, name: str = 'undefined', content: str = '') -> Dict:
        """Add new tab

        Args:
            index (int, optional): index of new tab
            name (str, optional): name of new tab
            content (str, optional): content of new tab

        Returns:
            dict: new tab meta info: id, name and index
        """
        file_storage = self._get_file_storage()
        tab_id = self._get_next_tab_id()

        reorder_required = index is not None
        if index is None:
            all_tabs = self.get_all()
            if len(all_tabs) == 0:
                index = 0
            else:
                index = max([x.get('index', 0) for x in all_tabs]) + 1

        data_bytes = json.dumps({
            'index': index,
            'name': name,
            'content': content
        }).encode("utf-8")
        file_storage.file_set(f'tab_{tab_id}', data_bytes)

        if reorder_required:
            all_tabs = self.get_all()
            all_tabs.sort(key=lambda x: (x['index'], 0 if x['id'] == tab_id else 1))
            file_storage.sync = False
            for tab_index, tab in enumerate(all_tabs):
                tab['index'] = tab_index
                data_bytes = json.dumps(tab).encode('utf-8')
                file_storage.file_set(f'tab_{tab["id"]}', data_bytes)
            file_storage.sync = True
            file_storage.push()

        return {'id': tab_id, 'index': index, 'name': name}

    def modify(self, tab_id: int, index: int = None, name: str = None, content: str = None) -> Dict:
        """Modify the tab

        Args:
            tab_id (int): if of the tab to modify
            index (int, optional): tab's new index
            name (str, optional): tab's new name
            content (str, optional): tab's new content

        Returns:
            dict: new tab meta info: id, name and index
        """
        file_storage = self._get_file_storage()
        current_data = self.get(tab_id)

        # region modify index
        if index is not None and current_data['index'] != index:
            current_data['index'] = index
            all_tabs = [x for x in self.get_all() if x['id'] != tab_id]
            all_tabs.sort(key=lambda x: x['index'])
            file_storage.sync = False
            for tab_index, tab in enumerate(all_tabs):
                if tab_index < index:
                    tab['index'] = tab_index
                else:
                    tab['index'] = tab_index + 1
                data_bytes = json.dumps(tab).encode('utf-8')
                file_storage.file_set(f'tab_{tab["id"]}', data_bytes)
            file_storage.sync = True
            file_storage.push()
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
        file_storage.file_set(f'tab_{tab_id}', data_bytes)

        return {
            'id': current_data['id'],
            'index': current_data['index'],
            'name': current_data['name']
        }

    def delete(self, tab_id: int):
        file_storage = self._get_file_storage()
        try:
            file_storage.file_get(f'tab_{tab_id}')
        except FileNotFoundError:
            raise EntityNotExistsError(f'tab {tab_id}')

        file_storage.delete(f'tab_{tab_id}')


tabs_controller = TabsController()
