import os
import time

import pandas as pd
import pytest
from mindsdb_sql import parse_sql

from tests.unit.executor_test_base import BaseExecutorTest

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
os.environ["OPENAI_API_KEY"] = OPENAI_API_KEY

WRITER_API_KEY = os.environ.get("WRITER_API_KEY")
os.environ["WRITER_API_KEY"] = WRITER_API_KEY

WRITER_ORG_ID = os.environ.get("WRITER_ORG_ID")
os.environ["WRITER_ORG_ID"] = WRITER_ORG_ID


class TestRAG(BaseExecutorTest):
    def wait_predictor(self, project, name):
        # wait
        done = False
        for attempt in range(200):
            ret = self.run_sql(f"select * from {project}.models where name='{name}'")
            if not ret.empty:
                if ret["STATUS"][0] == "complete":
                    done = True
                    break
                elif ret["STATUS"][0] == "error":
                    break
            time.sleep(0.5)
        if not done:
            raise RuntimeError("predictor wasn't created")

    def run_sql(self, sql):
        ret = self.command_executor.execute_command(parse_sql(sql, dialect="mindsdb"))
        assert ret.error_code is None
        if ret.data is not None:
            columns = [
                col.alias if col.alias is not None else col.name for col in ret.columns
            ]
            return pd.DataFrame(ret.data, columns=columns)

    def test_missing_required_keys(self):
        # create project
        self.run_sql("create database proj")

        self.run_sql(
            """
                CREATE MODEL proj.test_rag_handler_missing_required_args
                PREDICT answer
                USING
                   engine="rag"
                   """
        )
        with pytest.raises(Exception):
            self.wait_predictor("proj", "test_rag_handler_missing_required_args")

    def test_invalid_model_id_parameter(self):
        # create project

        self.run_sql("create database proj")
        self.run_sql(
            f"""
              create model proj.test_rag_openai_nonexistant_model
              predict answer
              using
                engine='rag',
                llm_type='openai',
                model_id='this-model-does-not-exist',
                openai_api_key='{OPENAI_API_KEY}';
           """
        )
        with pytest.raises(Exception):
            self.wait_predictor("proj", "test_rag_openai_nonexistant_model")

        self.run_sql(
            f"""
                  create model proj.test_rag_writer_nonexistant_model
                  predict answer
                  using
                    engine='rag',
                    llm_type='writer',
                    model_id='this-model-does-not-exist',
                    writer_api_key='{WRITER_API_KEY}',
                    writer_org_id='{WRITER_ORG_ID}';
               """
        )

        with pytest.raises(Exception):
            self.wait_predictor("proj", "test_rag_writer_nonexistant_model")

    def test_unsupported_llm_type(self):
        self.run_sql("create database proj")
        self.run_sql(
            """
            create model proj.test_unsupported_llm
            predict answer
            using
                engine='rag',
                llm_type='unsupported_llm'
        """
        )
        with pytest.raises(Exception):
            self.wait_predictor("proj", "test_unsupported_llm")

    def test_unsupported_vector_store(self):
        self.run_sql("create database proj")
        self.run_sql(
            f"""
            create model proj.test_unsupported_vector_store
            predict answer
            using
                engine='rag',
                llm_type='openai',
                openai_api_key='{OPENAI_API_KEY}',
                vector_store_name='unsupported_vector_store'
        """
        )

        with pytest.raises(Exception):
            self.wait_predictor("proj", "test_unsupported_vector_store")

    def test_unknown_arguments(self):
        self.run_sql("create database proj")
        self.run_sql(
            f"""
            create model proj.test_openai_unknown_arguments
            predict answer
            using
                engine='rag',
                llm_type='openai',
                openai_api_key='{OPENAI_API_KEY}',
                evidently_wrong_argument='wrong value'  --- this is a wrong argument name
        """
        )
        with pytest.raises(Exception):
            self.wait_predictor("proj", "test_openai_unknown_arguments")

    def test_qa(self):
        # create project
        self.run_sql("create database proj")
        df = pd.DataFrame.from_dict(
            {
                "context": [
                    "For adults and children age 5 and older, OTC decongestants, "
                    "antihistamines and pain relievers might offer some symptom relief. "
                    "However, they won't prevent a cold or shorten its duration, and most have some side effects.",
                    "Paracetamol, also known as acetaminophen and APAP, "
                    "is a medication used to treat pain and fever as well as colds and flu. "
                    "It is typically used for mild to moderate pain relief. "
                    "Evidence is mixed for its use to relieve fever in children. "
                    "It is often sold in combination with other medications, such as in many cold medications.",
                    "lemsip is a brand of over-the-counter pharmaceuticals used to treat cold and flu symptoms. "
                    "The brand is currently owned by Reckitt Benckiser. "
                    "The original Lemsip product contained paracetamol as its active ingredient. "
                    "However, other products marketed under the Lemsip "
                    "brand contain other active ingredients such as ibuprofen,"
                    "pseudoephedrine, phenylephrine, and guaifenesin."
                ],
                "url": [
                    "https://docs.mindsdb.com/sql/tutorials/recommenders/",
                    "https://docs.mindsdb.com/sql/tutorials/llm-chatbot-ui/",
                    "https://docs.mindsdb.com/sql/tutorials/house-sales-forecasting/",
                ],
            }
        )
        self.save_file("df", df)

        # test openai qa with chromadb

        self.run_sql(
            f"""
           create model proj.test_rag_openai_qa
           from files (select * from df)
           predict answer
           using
             engine='rag',
             llm_type='openai',
             openai_api_key='{OPENAI_API_KEY}',
             vector_store_folder_name='rag_openai_qa_test'
        """
        )
        self.wait_predictor("proj", "test_rag_openai_qa")

        result_df = self.run_sql(
            """
            SELECT p.answer
            FROM proj.test_rag_openai_qa as p
            WHERE question='What is the best treatment for a cold?'
        """
        )
        assert result_df["answer"].iloc[0]

        # test batching with openai qa chroma

        embeddings_batch_size = 1

        self.run_sql(
            f"""
           create model proj.test_rag_openai_qa_batch
           from files (select * from df)
           predict answer
           using
             engine='rag',
             llm_type='openai',
             openai_api_key='{OPENAI_API_KEY}',
             vector_store_folder_name='rag_openai_qa_test_batch',
             embeddings_batch_size={embeddings_batch_size}
        """
        )

        self.wait_predictor("proj", "test_rag_openai_qa_batch")

        result_df = self.run_sql(
            """
            SELECT p.answer
            FROM proj.test_rag_openai_qa_batch as p
            WHERE question='What is the best treatment for a cold?'
        """
        )
        assert result_df["answer"].iloc[0]

        # test writer qa with FAISS

        self.run_sql(
            f"""
           create model proj.test_rag_writer_qa
           from files (select * from df)
           predict answer
           using
             engine='rag',
             llm_type='writer',
             vector_store_name='faiss',
             writer_api_key='{WRITER_API_KEY}',
             writer_org_id='{WRITER_ORG_ID}',
             vector_store_folder_name='rag_writer_qa_test'
        """
        )
        self.wait_predictor("proj", "test_rag_writer_qa")

        result_df = self.run_sql(
            """
            SELECT p.answer
            FROM proj.test_rag_writer_qa as p
            WHERE question='What is the best treatment for a cold?'
        """
        )
        assert result_df["answer"].iloc[0]

        # test single url parsing
        self.run_sql(
            f"""
           create model proj.test_rag_writer_qa_single_url
           predict answer
           using
             engine='rag',
             llm_type='writer',
             url='https://docs.mindsdb.com/sql/tutorials/recommenders/',
             vector_store_name='faiss',
             writer_api_key='{WRITER_API_KEY}',
             writer_org_id='{WRITER_ORG_ID}',
             vector_store_folder_name='rag_writer_qa_test_single_url'
        """
        )
        self.wait_predictor("proj", "test_rag_writer_qa_single_url")

        result_df = self.run_sql(
            """
            SELECT p.answer
            FROM proj.test_rag_writer_qa as p
            WHERE question='What recommender models does mindsdb support?'
        """
        )
        assert result_df["answer"].iloc[0]

        # test multi url parsing
        self.run_sql(
            f"""
           create model proj.test_rag_writer_qa_multi_url
           from files (select * from df)
           predict answer
           using
             engine='rag',
             llm_type='writer',
             vector_store_name='faiss',
             url_column_name='url',
             writer_api_key='{WRITER_API_KEY}',
             writer_org_id='{WRITER_ORG_ID}',
             vector_store_folder_name='rag_writer_qa_test_multi_url'
        """
        )

        self.wait_predictor("proj", "test_rag_writer_qa_multi_url")

        result_df = self.run_sql(
            """
            SELECT p.answer
            FROM proj.test_rag_writer_qa_multi_url as p
            WHERE question='which chat app currently works with mindsdb chatbot?'
        """
        )

        assert result_df["answer"].iloc[0]

    def test_invalid_prompt_template(self):
        # create project
        self.run_sql("create database proj")
        self.run_sql(
            f"""
           create model proj.test_invalid_prompt_template_format
           predict completion
           using
             engine='rag',
             llm_type="openai",
             prompt_template="not valid format",
             openai_api_key='{OPENAI_API_KEY}';
        """
        )
        with pytest.raises(Exception):
            self.wait_predictor("proj", "test_invalid_prompt_template_format")
