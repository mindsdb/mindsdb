import ast
import json
from typing import List

import nltk
import pandas as pd
from integrations.handlers.writer_handler.question_answer import QuestionAnswerer
from integrations.handlers.writer_handler.settings import WriterHandlerParameters
from nltk import word_tokenize
from nltk.translate.bleu_score import (  # todo investigate why this always returns 0, not used for now
    sentence_bleu,
)
from nltk.translate.meteor_score import meteor_score
from rouge_score import rouge_scorer, scoring
from scipy.spatial import distance

from mindsdb.utilities.log import get_log

# todo use polars for this for speed
# todo refactor metric calculations operate on df columns
# todo remove remove repitition in metric calculations
# todo allow users to select subset of metrics to calculate

logger = get_log(logger_name=__name__)


class Evaluator:
    def __init__(self, args: WriterHandlerParameters, df: pd.DataFrame):

        self.args = args
        self.df = df
        self.question_answerer = QuestionAnswerer(args)

        if args.evaluation_type == "e2e":
            # todo check if this is fine for cloud, better to download once and load from disk
            nltk.download("wordnet")

    @property
    def metrics(self):
        """Get evaluation metrics"""
        return list(self.args.retrieval_evaluation_metrics) + list(
            self.args.generation_evaluation_metrics
        )

    def embed_texts(self, texts: List[str]) -> List[list]:
        """Embed a list of texts"""
        return self.question_answerer.embeddings_model.embed_documents(texts)

    def extract_returned_text(self, question: str):
        # todo: this is a hack, we need to fix this so it works with multiple context ie top_k>1

        vector_store_response = self.question_answerer.query_vector_store(question)

        return [doc.page_content for doc in vector_store_response][0]

    def evaluation_prompt(self, question: str, context: str):
        """Create prompt for evaluating RAG"""

        if self.args.summarize_context:

            return self.question_answerer.summarize_context(
                question=question, combined_context=context
            )

        return self.question_answerer.prompt_template.format(
            question=question, context=context
        )

    def get_evaluation_prompts(self, df: pd.DataFrame) -> List[str]:
        """Create prompts for each question and context pair in the dataframe"""
        return [
            self.evaluation_prompt(question, context)
            for question, context in zip(df["question"], df["retrieved_context"])
        ]

    @staticmethod
    def extract_generated_text(responses: List[str]):
        """Extract generated text from LLM response"""

        results = []
        for i, item in enumerate(responses):
            try:
                data = json.loads(item)
                if "choices" in data:
                    text = data["choices"][0]["text"]
                    results.append(text)
            except Exception as e:
                raise Exception(
                    f"{e} Error extracting generated text: failed to parse response {item}"
                )
        return results

    def mean_metrics_to_dict(self, df: pd.DataFrame):
        """get mean metric from df column to a dictionary"""

        return {
            metric: df[metric].mean() for metric in self.metrics if metric != "rouge"
        }

    def calculate_metrics(self, df: pd.DataFrame):
        """Calculate metrics for each question in the dataframe"""
        metrics = self.metrics
        ...

    @staticmethod
    def extract_reference_answers(df: pd.DataFrame) -> List[str]:
        """Get reference answers for each question in the dataframe"""

        # todo: this is a hack, we need to fix this so it works with multiple answers ie top_k>1
        answers = df["answers"].tolist()
        extracted_answers = []

        for answer in answers:
            try:
                extracted_answers.append(ast.literal_eval(answer)["text"][0])
            except IndexError as e:
                logger.error(e)
                extracted_answers.append("")
                continue

        return extracted_answers

    @staticmethod
    def _calculate_cosine_similarity(
        context_embeddings: List[float], retrieved_embeddings: List[float]
    ) -> float:
        """Calculate cosine similarity between a context and retrieved context embedding"""
        cosine_sim = 1 - distance.cosine(context_embeddings, retrieved_embeddings)

        return cosine_sim

    def calculate_cosine_similarities(
        self, context_embeddings: List[list], retrieved_context_embeddings: List[list]
    ):
        """Calculate cosine similarity for each context and retrieved context pair for a given question"""

        return [
            self._calculate_cosine_similarity(
                context_embedding, retrieved_context_embedding
            )
            for context_embedding, retrieved_context_embedding in zip(
                context_embeddings, retrieved_context_embeddings
            )
        ]

    @staticmethod
    def check_match(cosine_similarity: float, threshold: float = 0.7) -> int:

        return int(cosine_similarity >= threshold)

    @staticmethod
    def _tokenize(text: str) -> List[str]:
        """Tokenize a text"""
        return word_tokenize(text)

    @staticmethod
    def calculate_rouge(generated: str, reference: str) -> dict:

        scorer = rouge_scorer.RougeScorer(["rouge1", "rougeL"], use_stemmer=True)
        score = scorer.score(generated, reference)
        return score

    @staticmethod
    def extract_rogue_scores(df: pd.DataFrame, rogue_scores_col: str = "rouge_scores"):
        """Extract rouge scores from dataframe"""
        rouge_metrics = ["rouge1", "rougeL"]
        supported_metrics = ["precision", "recall", "fmeasure"]

        for rouge_metric in rouge_metrics:
            for supported_metric in supported_metrics:
                df[f"{rouge_metric}_{supported_metric}"] = df.apply(
                    lambda x: getattr(
                        x[rogue_scores_col][rouge_metric], supported_metric
                    ),
                    axis=1,
                )

    @staticmethod
    def calculate_bleu(
        generated_tokens: List[str], reference_tokens: List[str]
    ) -> float:
        return sentence_bleu([reference_tokens], generated_tokens)

    @staticmethod
    def calculate_meteor(
        generated_tokens: List[str], reference_tokens: List[str]
    ) -> float:
        return meteor_score([reference_tokens], generated_tokens)

    def evaluate_retrieval(self):
        """Evaluate the retrieval model"""
        # todo clean up

        df = self.df.copy(deep=True)

        # get question answering results
        df["retrieved_context"] = df.apply(
            lambda x: self.extract_returned_text(x["question"]), axis=1
        )

        # embed context and retrieved context
        context_embeddings = self.embed_texts(df["context"].tolist())
        retrieved_context_embeddings = self.embed_texts(
            df["retrieved_context"].tolist()
        )

        df["retrieval_cosine_similarity"] = self.calculate_cosine_similarities(
            context_embeddings, retrieved_context_embeddings
        )

        # calculate accuracy
        df["retrieval_match"] = df.apply(
            lambda x: self.check_match(
                x["retrieval_cosine_similarity"],
                threshold=self.args.retriever_accuracy_threshold,
            ),
            axis=1,
        )

        retrieval_accuracy = df["retrieval_match"].mean()
        retrieval_cosine_similarity = df["retrieval_cosine_similarity"].mean()

        logger.info(f"Retrieval Accuracy: {retrieval_accuracy}")
        logger.info(f"Retrieval Cosine Similarity: {retrieval_cosine_similarity}")

        return df

    def evaluate_generation(self, df: pd.DataFrame):
        """Evaluate the generation model, given the retrieval results df"""

        # todo clean up

        prompts = self.get_evaluation_prompts(df)

        raw_generated_answers = [
            self.question_answerer.llm(prompt) for prompt in prompts
        ]

        generated_answers = self.extract_generated_text(raw_generated_answers)
        reference_answers = self.extract_reference_answers(df)

        df["generated_answers"] = generated_answers
        df["reference_answers"] = reference_answers

        # tokenize generated and reference answers
        df["tokenized_generated_answers"] = df.apply(
            lambda x: self._tokenize(x["generated_answers"]), axis=1
        )
        df["tokenized_reference_answers"] = df.apply(
            lambda x: self._tokenize(x["reference_answers"]), axis=1
        )

        # embed generated answers and reference answers
        generated_answer_embeddings = self.embed_texts(generated_answers)

        reference_answer_embeddings = self.embed_texts(reference_answers)

        # calculate cosine similarity between generated and reference answers
        df["generator_cosine_similarity"] = self.calculate_cosine_similarities(
            generated_answer_embeddings, reference_answer_embeddings
        )

        # calculate accuracy
        df["generator_match"] = df.apply(
            lambda x: self.check_match(
                x["generator_cosine_similarity"],
                threshold=self.args.generator_accuracy_threshold,
            ),
            axis=1,
        )

        # calculate rouge scores
        df["rouge_scores"] = df.apply(
            lambda x: self.calculate_rouge(
                x["generated_answers"], x["reference_answers"]
            ),
            axis=1,
        )

        self.extract_rogue_scores(df)

        # calculate meteor scores
        df["meteor_scores"] = df.apply(
            lambda x: self.calculate_meteor(
                x["tokenized_generated_answers"], x["tokenized_reference_answers"]
            ),
            axis=1,
        )

        return df

    def evaluate_e2e(self):
        """Evaluate the end-to-end evaluation"""
        retrieval_df = self.evaluate_retrieval()
        e2e_df = self.evaluate_generation(retrieval_df)

        return e2e_df

    def evaluate(self):
        if self.args.evaluation_type == "retrieval":
            return self.evaluate_retrieval()
        elif self.args.evaluation_type == "e2e":
            return self.evaluate_e2e()
        else:
            raise ValueError(f"evaluation_type must be either 'retrieval' or 'e2e'")
