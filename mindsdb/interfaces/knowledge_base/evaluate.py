import json
import math
import re
import time
from typing import List

import pandas as pd
import datetime as dt

from mindsdb.api.executor.sql_query.result_set import ResultSet
from mindsdb_sql_parser import Identifier, Select, Constant, Star, parse_sql, BinaryOperation
from mindsdb.utilities import log

from mindsdb.interfaces.knowledge_base.llm_client import LLMClient

logger = log.getLogger(__name__)


GENERATE_QA_SYSTEM_PROMPT = """
Your task is to generate question and answer pairs for a search engine.
The search engine will take your query and return a list of documents.
You will be given a text and you need to generate a question that can be answered using the information in the text.
Your questions will be used to evaluate the search engine.
Question should always have enough clues to identify the specific text that this question is generated from.
Never ask questions like "What license number is associated with Amend 6" because Amend 6 could be found in many documents and the question is not specific enough.
Example output 1:  {\"query\": \"What processor does the HP 2023 14\" FHD IPS Laptop use?\", \"reference_answer\": \"Ryzen 3 5300U\"}
Example output 2: {\"query\": \"What is the name of the river in Paris?\", \"reference_answer\": \"Seine\"}
Don't generate questions like "What is being amended in the application?" because these questions cannot be answered using the text and without knowing which document it refers to.
The question should be answerable without the text, but the answer should be present in the text.
Return ONLY a json response. No other text.
"""


def calc_entropy(values: List[float]) -> float:
    """
    Alternative of scipy.stats.entropy, to not add `scipy` dependency
    :param values: Input distribution
    :return: The calculated entropy.
    """
    # normalize & filter
    total = sum(values)
    values = [i / total for i in values if i > 0]
    # calc
    return -sum([pk * math.log(pk) for pk in values])


def sanitize_json_response(response: str) -> str:
    """Remove markdown code block formatting from JSON response and extract valid JSON."""
    if not response or not response.strip():
        raise ValueError("Empty response provided.")

    # Remove leading/trailing whitespace
    response = response.strip()

    # Remove markdown code block markers if present
    response = re.sub(r"^```(?:json|JSON)?\s*", "", response, flags=re.MULTILINE)
    response = re.sub(r"\s*```$", "", response, flags=re.MULTILINE)
    response = response.strip()

    # Find the first opening brace
    start_idx = response.find("{")
    if start_idx == -1:
        raise ValueError("No JSON object found in the response.")

    # Try to parse JSON starting from first { with increasing end positions
    # This handles nested objects and strings with braces correctly
    for end_idx in range(len(response), start_idx, -1):  # Start from end and work backwards
        candidate = response[start_idx:end_idx]
        try:
            parsed = json.loads(candidate)
            # Ensure it's a dictionary (object) not just any valid JSON
            if isinstance(parsed, dict):
                return candidate
        except json.JSONDecodeError:
            continue

    raise ValueError("No valid JSON object found in the response.")


class EvaluateBase:
    DEFAULT_QUESTION_COUNT = 20
    DEFAULT_SAMPLE_SIZE = 10000

    def __init__(self, session, knowledge_base):
        self.kb = knowledge_base
        self.name = knowledge_base._kb.name
        self.session = session

        self._llm_client = None

    def generate(self, sampled_df: pd.DataFrame) -> pd.DataFrame:
        # generate test data from sample
        raise NotImplementedError

    def evaluate(self, test_data: pd.DataFrame) -> pd.DataFrame:
        # create evaluate metric from test data
        raise NotImplementedError

    def _set_llm_client(self, llm_params: dict):
        """
        Logic to get LLM setting:
        - first get `llm` setting of ‘evaluate’ command
        - if not defined, look at the knowledge base reranker config
        """
        if llm_params is None:
            llm_params = self.kb._kb.params.get("reranking_model")

        self.llm_client = LLMClient(llm_params)

    def generate_test_data(self, gen_params: dict) -> pd.DataFrame:
        # Extract source data (from users query or from KB itself) and call `generate` to get test data

        if "from_sql" in gen_params:
            # get data from sql
            query = parse_sql(gen_params["from_sql"])
            if not isinstance(query, Select) or query.from_table is None:
                raise ValueError(f"Query not supported {gen_params['from_sql']}")

            dn, table_name = self._get_dn_table(query.from_table)
            query.from_table = table_name
            if query.limit is None:
                query.limit = Constant(self.DEFAULT_SAMPLE_SIZE)

            response = dn.query(query=query, session=self.session)
            df = response.data_frame

            if "content" not in df.columns:
                raise ValueError(f"`content` column isn't found in provided sql: {gen_params['from_sql']}")

            df.rename(columns={"content": "chunk_content"}, inplace=True)
        else:
            # get data from knowledge base
            df = self.kb.select_query(
                Select(
                    targets=[Identifier("chunk_content"), Identifier("id")], limit=Constant(self.DEFAULT_SAMPLE_SIZE)
                )
            )

        if "count" in gen_params:
            number_of_questions = gen_params["count"]
        else:
            number_of_questions = self.DEFAULT_QUESTION_COUNT

        number_of_questions = min(number_of_questions, len(df))
        sampled_df = df.sample(n=number_of_questions)

        return self.generate(sampled_df)

    def read_from_table(self, test_table: Identifier) -> pd.DataFrame:
        # read data from table

        dn, table_name = self._get_dn_table(test_table)

        query = Select(
            targets=[Star()],
            from_table=table_name,
        )
        response = dn.query(query=query, session=self.session)
        return response.data_frame

    def _get_dn_table(self, table_name: Identifier):
        if len(table_name.parts) < 2:
            raise ValueError(f"Can't find database, table name must have at least 2 parts: {table_name}")

        integration_name = table_name.parts[0]
        table_name = Identifier(parts=table_name.parts[1:])
        dn = self.session.datahub.get(integration_name)
        if dn is None:
            raise ValueError(f"Can't find database: {integration_name}")
        return dn, table_name

    def save_to_table(self, table_name: Identifier, df: pd.DataFrame, is_replace=False):
        # save data to table

        dn, table_name = self._get_dn_table(table_name)

        data = ResultSet.from_df(df)

        dn.create_table(
            table_name=table_name,
            result_set=data,
            is_replace=is_replace,
            is_create=True,
            raise_if_exists=False,
        )

    def run_evaluate(self, params: dict) -> pd.DataFrame:
        # evaluate function entry point

        self._set_llm_client(params.get("llm"))

        if "test_table" not in params:
            raise ValueError('The table with  has to be defined in "test_table" parameter')

        test_table = params["test_table"]

        if isinstance(test_table, str):
            test_table = Identifier(test_table)

        if "generate_data" in params:
            # generate question / answers using llm
            gen_params = params["generate_data"]
            if not isinstance(gen_params, dict):
                gen_params = {}
            test_data = self.generate_test_data(gen_params)

            self.save_to_table(test_table, test_data, is_replace=True)

        if params.get("evaluate", True) is False:
            # no evaluate is required
            return pd.DataFrame()

        test_data = self.read_from_table(test_table)

        scores = self.evaluate(test_data)
        scores["id"] = math.floor(time.time())  # unique ID for the evaluation run
        scores["name"] = self.name
        scores["created_at"] = dt.datetime.now()

        # save scores
        if "save_to" in params:
            to_table = params["save_to"]
            if isinstance(to_table, str):
                to_table = Identifier(to_table)
            self.save_to_table(to_table, scores.copy())

        return scores

    @staticmethod
    def run(session, kb_table, params) -> pd.DataFrame:
        # choose the evaluator version according to the 'version' parameter in config

        evaluate_version = params.get("version", "doc_id")

        if evaluate_version == "llm_relevancy":
            cls = EvaluateRerank
        elif evaluate_version == "doc_id":
            cls = EvaluateDocID
        else:
            raise NotImplementedError(f"Version of evaluator is not implemented: {evaluate_version}")

        return cls(session, kb_table).run_evaluate(params)


class EvaluateRerank(EvaluateBase):
    """
    Rank responses from KB using LLM (by calling KB reranker function)
    """

    TOP_K = 10

    def generate(self, sampled_df: pd.DataFrame) -> pd.DataFrame:
        qa_data = []
        count_errors = 0
        for chunk_content in sampled_df["chunk_content"]:
            try:
                question, answer = self.generate_question_answer(chunk_content)
            except ValueError as e:
                # allow some numbers of error
                count_errors += 1
                if count_errors > 5:
                    raise e
                continue

            qa_data.append({"text": chunk_content, "question": question, "answer": answer})

        df = pd.DataFrame(qa_data)
        df["id"] = df.index
        return df

    def generate_question_answer(self, text: str) -> (str, str):
        messages = [
            {"role": "system", "content": GENERATE_QA_SYSTEM_PROMPT},
            {"role": "user", "content": f"\n\nText:\n{text}\n\n"},
        ]
        answer = self.llm_client.completion(messages, json_output=True)

        # Sanitize the response by removing markdown code block formatting like ```json
        sanitized_answer = sanitize_json_response(answer)

        try:
            output = json.loads(sanitized_answer)
        except json.JSONDecodeError:
            raise ValueError(f"Could not parse response from LLM: {answer}")

        if "query" not in output or "reference_answer" not in output:
            raise ValueError("Cant find question/answer in LLM response")

        return output.get("query"), output.get("reference_answer")

    def evaluate(self, test_data: pd.DataFrame) -> pd.DataFrame:
        json_to_log_list = []
        questions = test_data.to_dict("records")

        for i, item in enumerate(questions):
            question = item["question"]
            ground_truth = item["answer"]

            start_time = time.time()
            logger.debug(f"Querying [{i + 1}/{len(questions)}]: {question}")
            df_answers = self.kb.select_query(
                Select(
                    targets=[Identifier("chunk_content")],
                    where=BinaryOperation(op="=", args=[Identifier("content"), Constant(question)]),
                    limit=Constant(self.TOP_K),
                )
            )
            query_time = time.time() - start_time

            proposed_responses = list(df_answers["chunk_content"])

            # generate answer using llm
            relevance_score_list = self.kb.score_documents(question, proposed_responses, self.llm_client.params)

            # set binary relevancy
            binary_relevancy_list = [1 if score >= 0.5 else 0 for score in relevance_score_list]

            # calculate first relevant position
            first_relevant_position = next((i for i, x in enumerate(binary_relevancy_list) if x == 1), None)
            json_to_log = {
                "question": question,
                "ground_truth": ground_truth,
                # "relevancy_at_k": relevancy_at_k,
                "binary_relevancy_list": binary_relevancy_list,
                "relevance_score_list": relevance_score_list,
                "first_relevant_position": first_relevant_position,
                "query_time": query_time,
            }
            json_to_log_list.append(json_to_log)

        evaluation_results = self.evaluate_retrieval_metrics(json_to_log_list)
        return pd.DataFrame([evaluation_results])

    def evaluate_retrieval_metrics(self, json_to_log_list):
        """
        Computes retrieval evaluation metrics from the result log.

        Metrics computed:
            - Average Relevancy (mean soft relevance score)
            - Average Relevancy@k (soft score)
            - Average First Relevant Position
            - Mean Reciprocal Rank (MRR)
            - Hit@k
            - Binary Precision@k
            - Average Entropy of Relevance Scores
            - Average nDCG

        Args:
            json_to_log_list (list): List of evaluation logs per query.

        Returns:
            dict: A dictionary containing all computed metrics.
        """

        mrr_list = []
        hit_at_k_matrix = []
        binary_precision_at_k_matrix = []
        ndcg_list = []
        entropy_list = []

        total_relevancy = 0
        relevance_score_matrix = []
        first_relevant_positions = []

        for item in json_to_log_list:
            binary_relevancy = item["binary_relevancy_list"]
            relevance_scores = item["relevance_score_list"]

            # Skip if empty
            if not relevance_scores:
                continue

            # Mean relevancy per query
            query_relevancy = sum(relevance_scores) / len(relevance_scores)
            total_relevancy += query_relevancy

            # Build score matrix for later average@k
            relevance_score_matrix.append(relevance_scores)

            # First relevant position
            pos = item["first_relevant_position"]
            if pos is not None:
                first_relevant_positions.append(pos)

            # MRR
            reciprocal_rank = 1 / (pos + 1) if pos is not None else 0
            mrr_list.append(reciprocal_rank)

            # Hit@k and Binary Precision@k
            hit_row = []
            precision_row = []
            for k in range(1, len(binary_relevancy) + 1):
                hit = int(any(binary_relevancy[:k]))
                precision = sum(binary_relevancy[:k]) / k
                hit_row.append(hit)
                precision_row.append(precision)
            hit_at_k_matrix.append(hit_row)
            binary_precision_at_k_matrix.append(precision_row)

            # Entropy

            entropy = calc_entropy(relevance_scores) if len(relevance_scores) > 1 else 0
            entropy_list.append(entropy)

            # nDCG
            def dcg(scores):
                return sum(score / math.log2(idx + 2) for idx, score in enumerate(scores))

            ideal = sorted(relevance_scores, reverse=True)
            actual_dcg = dcg(relevance_scores)
            ideal_dcg = dcg(ideal)
            ndcg = actual_dcg / ideal_dcg if ideal_dcg > 0 else 0
            ndcg_list.append(ndcg)

        # Aggregated metrics
        num_queries = len(json_to_log_list)
        average_relevancy = total_relevancy / num_queries if num_queries else 0

        # Relevancy@k
        average_relevance_score_by_k = []
        if relevance_score_matrix:
            relevance_score_matrix = list(zip(*relevance_score_matrix))
            for col in relevance_score_matrix:
                avg_k = sum(col) / len(col)
                average_relevance_score_by_k.append(round(avg_k, 2))

        average_first_relevant_position = (
            sum(first_relevant_positions) / len(first_relevant_positions) if first_relevant_positions else None
        )

        mean_mrr = sum(mrr_list) / len(mrr_list) if mrr_list else 0
        hit_at_k_avg = [round(sum(col) / len(col), 2) for col in zip(*hit_at_k_matrix)] if hit_at_k_matrix else []
        binary_precision_at_k_avg = (
            [round(sum(col) / len(col), 2) for col in zip(*binary_precision_at_k_matrix)]
            if binary_precision_at_k_matrix
            else []
        )
        avg_entropy = sum(entropy_list) / len(entropy_list) if entropy_list else 0
        avg_ndcg = sum(ndcg_list) / len(ndcg_list) if ndcg_list else 0

        avg_query_time = sum(item["query_time"] for item in json_to_log_list) / num_queries

        return {
            "avg_relevancy": average_relevancy,
            "avg_relevance_score_by_k": average_relevance_score_by_k,
            "avg_first_relevant_position": average_first_relevant_position,
            "mean_mrr": mean_mrr,
            "hit_at_k": hit_at_k_avg,
            "bin_precision_at_k": binary_precision_at_k_avg,
            "avg_entropy": avg_entropy,
            "avg_ndcg": avg_ndcg,
            "avg_query_time": avg_query_time,
        }


class EvaluateDocID(EvaluateBase):
    """
    Checks if ID in response from KB is matched with doc ID in test dataset
    """

    TOP_K = 20

    def generate(self, sampled_df: pd.DataFrame) -> pd.DataFrame:
        if "id" not in sampled_df.columns:
            raise ValueError("'id' column is required for generating test dataset")

        qa_data = []
        count_errors = 0
        for _, item in sampled_df.iterrows():
            chunk_content = item["chunk_content"]
            try:
                question, answer = self.generate_question_answer(chunk_content)
            except ValueError as e:
                # allow some numbers of error
                count_errors += 1
                if count_errors > 5:
                    raise e
                continue

            qa_data.append({"text": chunk_content, "question": question, "answer": answer, "doc_id": item["id"]})
        if len(qa_data) == 0:
            raise ValueError("No data in generated test dataset")
        df = pd.DataFrame(qa_data)
        return df

    def generate_question_answer(self, text: str) -> (str, str):
        messages = [
            {"role": "system", "content": GENERATE_QA_SYSTEM_PROMPT},
            {"role": "user", "content": f"\n\nText:\n{text}\n\n"},
        ]
        answer = self.llm_client.completion(messages, json_output=True)

        # Sanitize the response by removing markdown code block formatting like ```json
        sanitized_answer = sanitize_json_response(answer)

        try:
            output = json.loads(sanitized_answer)
        except json.JSONDecodeError:
            raise ValueError(f"Could not parse response from LLM: {answer}")

        if "query" not in output or "reference_answer" not in output:
            raise ValueError("Cant find question/answer in LLM response")

        return output.get("query"), output.get("reference_answer")

    def evaluate(self, test_data: pd.DataFrame) -> pd.DataFrame:
        stats = []
        questions = test_data.to_dict("records")

        for i, item in enumerate(questions):
            question = item["question"]
            doc_id = item["doc_id"]

            start_time = time.time()
            logger.debug(f"Querying [{i + 1}/{len(questions)}]: {question}")
            df_answers = self.kb.select_query(
                Select(
                    targets=[Identifier("chunk_content"), Identifier("id")],
                    where=BinaryOperation(op="=", args=[Identifier("content"), Constant(question)]),
                    limit=Constant(self.TOP_K),
                )
            )
            query_time = time.time() - start_time

            retrieved_doc_ids = list(df_answers["id"])

            if doc_id in retrieved_doc_ids:
                doc_found = True
                doc_position = retrieved_doc_ids.index(doc_id)
            else:
                doc_found = False
                doc_position = -1

            stats.append(
                {
                    "question": question,
                    "doc_id": doc_id,
                    "doc_found": doc_found,
                    "doc_position": doc_position,
                    "query_time": query_time,
                }
            )

        evaluation_results = self.summarize_results(stats)
        return pd.DataFrame([evaluation_results])

    def summarize_results(self, stats):
        total_questions = len(stats)
        total_found = sum([1 for stat in stats if stat["doc_found"]])

        accurate_in_top_10 = sum([1 for stat in stats if stat["doc_found"] and stat["doc_position"] < 10])

        # calculate recall curve by position
        recall_curve = {}
        for i in range(self.TOP_K):
            recall_curve[i] = sum([1 for stat in stats if stat["doc_found"] and stat["doc_position"] == i])
        # convert to proportion of total questions
        for i in range(self.TOP_K):
            recall_curve[i] = recall_curve[i] / total_questions
        # calculate cumulative recall
        cumulative_recall = {}
        for i in range(self.TOP_K):
            cumulative_recall[i] = sum([recall_curve[j] for j in range(i + 1)])

        avg_query_time = sum(item["query_time"] for item in stats) / total_questions
        return {
            "total": total_questions,
            "total_found": total_found,
            "retrieved_in_top_10": accurate_in_top_10,
            "cumulative_recall": json.dumps(cumulative_recall),
            "avg_query_time": avg_query_time,
        }
