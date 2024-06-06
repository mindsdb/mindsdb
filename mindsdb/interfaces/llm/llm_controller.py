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
        self.session.add(new_llm_data)
        self.session.commit()
        return new_llm_data

    # def get_llm_data(self) -> db.LLMData:
    #     '''
    #     Retrieves LLM data by ID.

    #     Parameters:
    #         llm_data_id (int): The ID of the LLM data entry.

    #     Returns:
    #         LLMData: The LLMData object from the database.
    #     '''
    #     return self.session.query(db.LLMData).all() # check on this

    # def update_llm_data(self, llm_data_id: int, new_input_data: str = None, new_output_data: str = None):
    #     '''
    #     Updates LLM data in the database.

    #     Parameters:
    #         llm_data_id (int): The ID of the LLM data to update.
    #         new_input_data (str): The new input to the LLM, if any.
    #         new_output_data (str): The new output from the LLM, if any.
    #     '''
    #     llm_data = self.get_llm_data(llm_data_id)
    #     if llm_data is None:
    #         raise ValueError("LLM Data not found")

    #     if new_input_data is not None:
    #         llm_data.input = new_input_data

    #     if new_output_data is not None:
    #         llm_data.output = new_output_data

    #     self.session.commit()

    def delete_llm_data(self, llm_data_id: int):
        '''
        Deletes LLM data from the database.
        Parameters:
            llm_data_id (int): The ID of the LLM data to delete.
        '''
        llm_data = self.get_llm_data(llm_data_id)
        if llm_data is None:
            raise ValueError("LLM Data not found")

        self.session.delete(llm_data)
        self.session.commit()

    def list_all_llm_data(self):
        '''
        Lists all LLM data entries.
        Returns:
            List[LLMData]: A list of all LLMData objects in the database.
        '''
        return self.session.query(db.LLMData).all()