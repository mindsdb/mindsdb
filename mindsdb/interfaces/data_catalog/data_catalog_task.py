import time

from mindsdb.api.executor.controllers.session_controller import SessionController
from mindsdb.interfaces.data_catalog.data_catalog_loader import DataCatalogLoader
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.interfaces.storage import db
from mindsdb.interfaces.tasks.task import BaseTask
from mindsdb.utilities import log
from mindsdb.utilities.config import config


logger = log.getLogger(__name__)


class DataCatalogTask(BaseTask):
    """
    The task to update the data catalog metadata in order to track changes in the underlying schema of the data source.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.session = SessionController()

    def run(self, stop_event):
        """
        Runs the task to update the data catalog metadata.
        Updates the metadata only for data sources where schema changes are possible, i.e. databases.
        """
        while not stop_event.is_set():
            try:
                logger.info(f"Updating data catalog metadata...")
                # Extract the data sources for which metadata should be updated.
                data_sources = db.MetaTables.query.join(db.Integration).with_entities(db.Integration.name).distinct().all()

                for data_source in data_sources:
                    data_source_name = data_source[0]
                    logger.info(f"Updating data catalog metadata for {data_source_name}...")

                    data_handler = self.session.integration_controller.get_data_handler(data_source_name)
                    # The schema of APIs will typically not change.
                    # TODO: However, the underlying data may change. Should we account for this?
                    if not isinstance(data_handler, APIHandler):
                        data_catalog_loader = DataCatalogLoader(
                            database_name=data_source_name,
                            data_handler=data_handler,
                        )
                        data_catalog_loader.load_metadata()
            except Exception as e:
                logger.error(f"Error updating data catalog metadata: {e}")

            if stop_event.is_set():
                return

            time.sleep(config.get("data_catalog").get("update_interval"))