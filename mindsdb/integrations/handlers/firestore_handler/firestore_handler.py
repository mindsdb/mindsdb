from typing import Any, Dict, Union
from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.response import HandlerStatusResponse

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore


class FirestoreHandler(DatabaseHandler):
    name = "firestore"

    def __init__(
        self,
        name: str,
        cert: Union[str, None] = None,
        options: Dict[str, Any] = {},
    ):
        """
        __init__ Initialize the FirestoreHandler class

        Parameters
        ----------
        name : str
            The name of the firestore app.
        cert : str
            The path to the certificate file or a dict representing the contents of the certificate.
            Defaults to the Google Application Default Credentials
        options : Dict[str, Any], optional
            _description_, by default {}
        """
        super().__init__(name)

        self.name = name
        self.app = None
        self.db = None
        if cert is None:
            self.__cred = credentials.ApplicationDefault()
        else:
            self.__cred = credentials.Certificate(cert)
        self.__options = options

    def connect(self) -> HandlerStatusResponse:
        reponse = HandlerStatusResponse(True)
        try:
            self.app = firebase_admin.initialize_app(
                name=self.name,
                credential=self.__cred,
                options=self.__options,
            )
            self.db = firestore.client()
            reponse.success = True
        except Exception as e:
            reponse.success = False
            reponse.error_message = str(e)
        return reponse

    def disconnect(self) -> None:
        if self.app is None:
            raise ConnectionError("You need to connect to the database first")
        firebase_admin.delete_app(self.app)
        self.name = None
        self.app = None
        self.db = None
        self.__cred = None
        self.__options = None

    def create_collection(self, collection_name: str) -> None:
        if self.db is None:
            self.connect()
            self.db.collection(collection_name)
            self.disconnect()
        else:
            self.db.collection(collection_name)

    def create_document(
        self, collection_name: str, document_name: str, data: Any
    ) -> None:
        if self.db is None:
            raise ConnectionError("You need to connect to the database first")
        self.db.collection(collection_name).document(document_name).set(data)

    def check_connection(self) -> HandlerStatusResponse:
        response = HandlerStatusResponse(False)
        try:
            self.connect()
            response.success = True
            self.disconnect()
        except Exception as e:
            response.error_message = str(e)
        return response

    def get_name(self) -> Union[str, None]:
        return self.name

    def get_project_id(self) -> str:
        if self.app is None:
            raise ConnectionError("You need to connect to the database first")
        return str(self.app.project_id)

    def get_collections(self):
        if self.db is None:
            raise ConnectionError("You need to connect to the database first")
        return self.db.collections()

    def get_documents(self, collection_name: str):
        if self.db is None:
            raise ConnectionError("You need to connect to the database first")
        return self.db.collection(collection_name).get()
