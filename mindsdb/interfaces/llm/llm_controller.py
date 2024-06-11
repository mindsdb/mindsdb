from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session

from mindsdb.interfaces.storage import db
from mindsdb.utilities.config import Config


class LLMDataController:
    '''Handles CRUD operations at the database level for LLM Data'''

    def __init__(self) -> None:
        '''Initializes the LLMDataController with configuration settings.'''
        self.config = Config()

    def add_llm_data(self, input_data: str, output_data: str) -> db.LLMData:
        '''
        Adds LLM input and output data to the database.
        Parameters:
            input_data (str): The input to the LLM.
            output_data (str): The output from the LLM.
        Returns:
            LLMData: The created LLMData object.
        '''
        new_llm_data = db.LLMData(
            input=input_data,
            output=output_data
        )
        db.session.add(new_llm_data)
        db.session.commit()
        return new_llm_data

    def delete_llm_data(self, llm_data_id: int):
        '''
        Deletes LLM data from the database.
        Parameters:
            llm_data_id (int): The ID of the LLM data to delete.
        '''
        llm_data = self.get_llm_data(llm_data_id)
        if llm_data is None:
            raise ValueError("LLM Data not found")

        db.session.delete(llm_data)
        db.session.commit()

    def list_all_llm_data(self):
        '''
        Lists all LLM data entries.
        Returns:
            List[LLMData]: A list of all LLMData objects in the database.
        '''
        return db.session.query(db.LLMData).all()