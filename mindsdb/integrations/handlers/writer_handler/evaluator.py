import ast
from time import sleep
from typing import List

import nltk
import pandas as pd
from integrations.handlers.writer_handler.question_answer import QuestionAnswerer
from integrations.handlers.writer_handler.settings import WriterHandlerParameters
from nltk import word_tokenize
from nltk.translate.bleu_score import sentence_bleu
from nltk.translate.meteor_score import meteor_score
from rouge_score import rouge_scorer
from scipy.spatial import distance

from mindsdb.utilities.log import get_log

# todo cache or store to avoid downloading every time


logger = get_log(logger_name=__name__)


MAX_REQUESTS_PER_MINUTE = 60


class Evaluator:
    def __init__(self, args: WriterHandlerParameters, df: pd.DataFrame):

        self.args = args
        self.df = df
        self.question_answerer = QuestionAnswerer(args)

    def extract_returned_text(self, question: str):

        vector_store_response = self.question_answerer.query_vector_store(question)

        return [doc.page_content for doc in vector_store_response][0]

    def evaluation_prompt(self, question: str, context: str):
        """Create prompt for evaluating RAG"""

        if self.args.summarize_context:
            return self.question_answerer._summarize_context(
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

    def get_reference_answers(self, df: pd.DataFrame) -> List[str]:
        """Get reference answers for each question in the dataframe"""

        return df.apply(
            lambda x: ast.literal_eval(x["reference_answers"])["text"][0], axis=1
        ).tolist()

    def rate_limited_llm_requests(self, prompts: list[str]):
        """Make LLM requests respecting rate limit."""
        llm_responses = []
        requests_this_minute = 0

        for prompt in prompts:
            if requests_this_minute < MAX_REQUESTS_PER_MINUTE:
                response = self.question_answerer.llm(prompt)
                llm_responses.append(response)
                requests_this_minute += 1
            else:
                wait_time = 60 - requests_this_minute
                logger.info(f"Rate limit reached. Waiting {wait_time} seconds.")
                sleep(wait_time)
                requests_this_minute = 0

        return llm_responses

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
    def accuracy(cosine_similarity: float, threshold: float = 0.7) -> bool:
        return cosine_similarity >= threshold

    @staticmethod
    def tokenize(text: str) -> List[str]:
        nltk.download("wordnet")
        return word_tokenize(text)

    @staticmethod
    def calculate_rouge(generated: str, reference: str) -> dict:
        scorer = rouge_scorer.RougeScorer(["rouge1", "rougeL"], use_stemmer=True)
        score = scorer.score(generated, reference)
        return score

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

        df = self.df.copy(deep=True)

        # get question answering results
        df["retrieved_context"] = df.apply(
            lambda x: self.extract_returned_text(x["question"]), axis=1
        )

        # embed context and retrieved context
        context_embeddings = self.question_answerer.embeddings_model.embed_documents(
            df["context"].tolist()
        )
        retrieved_context_embeddings = (
            self.question_answerer.embeddings_model.embed_documents(
                df["retrieved_context"].tolist()
            )
        )

        df["retrival_cosine_similarity"] = self.calculate_cosine_similarities(
            context_embeddings, retrieved_context_embeddings
        )

        # calculate accuracy
        df["retrieval_accuracy"] = df.apply(
            lambda x: self.accuracy(
                x["retrival_cosine_similarity"],
                threshold=self.args.retriever_accuracy_threshold,
            ),
            axis=1,
        )

        retrieval_accuracy = df["retrieval_accuracy"].mean()
        retrieval_cosine_similarity = df["retrival_cosine_similarity"].mean()

        evaluation_metrics = {
            "retrieval_accuracy": retrieval_accuracy,
            "retrieval_cosine_similarity": retrieval_cosine_similarity,
        }
        logger.info(f"Retrieval Accuracy: {retrieval_accuracy}")
        logger.info(f"Retrieval Cosine Similarity: {retrieval_cosine_similarity}")

        return evaluation_metrics, df

    def evaluate_e2e(self):

        evaluation_metrics, evaluation_df = self.evaluate_retrieval()

        prompts = self.get_evaluation_prompts(evaluation_df)

        generated_answers = self.rate_limited_llm_requests(prompts)
        reference_answers = self.get_reference_answers(evaluation_df)

        # embed generated answers and reference answers
        generated_answer_embeddings = (
            self.question_answerer.embeddings_model.embed_documents(generated_answers)
        )

        reference_answer_embeddings = (
            self.question_answerer.embeddings_model.embed_documents(reference_answers)
        )

        # calculate cosine similarity between generated and reference answers
        evaluation_df[
            "generated_cosine_similarity"
        ] = self.calculate_cosine_similarities(
            generated_answer_embeddings, reference_answer_embeddings
        )

        # calculate rouge scores
        evaluation_df["rouge_scores"] = evaluation_df.apply(
            lambda x: self.calculate_rouge(x["answer"], x["reference_answers"]), axis=1
        )

        # calculate bleu scores
        evaluation_df["bleu_scores"] = evaluation_df.apply(
            lambda x: self.calculate_bleu(
                self.tokenize(x["answer"]), self.tokenize(x["reference_answers"])
            ),
            axis=1,
        )

        # calculate meteor scores
        evaluation_df["meteor_scores"] = evaluation_df.apply(
            lambda x: self.calculate_meteor(
                self.tokenize(x["answer"]), self.tokenize(x["reference_answers"])
            ),
            axis=1,
        )

        # calculate accuracy
        evaluation_df["accuracy"] = evaluation_df.apply(
            lambda x: self.accuracy(
                x["generated_cosine_similarity"],
                threshold=self.args.e2e_accuracy_threshold,
            ),
            axis=1,
        )

        evaluation_metrics["generated_accuracy"] = evaluation_df["accuracy"].mean()
        evaluation_metrics["generated_cosine_similarity"] = evaluation_df[
            "generated_cosine_similarity"
        ].mean()
        evaluation_metrics["generated_rouge_scores"] = evaluation_df[
            "rouge_scores"
        ].mean()
        evaluation_metrics["generated_bleu_scores"] = evaluation_df[
            "bleu_scores"
        ].mean()
        evaluation_metrics["generated_meteor_scores"] = evaluation_df[
            "meteor_scores"
        ].mean()

        logger.info(f"Generated Accuracy: {evaluation_metrics['generated_accuracy']}")
        logger.info(
            f"Generated Cosine Similarity: {evaluation_metrics['generated_cosine_similarity']}"
        )
        logger.info(
            f"Generated Rouge Scores: {evaluation_metrics['generated_rouge_scores']}"
        )
        logger.info(
            f"Generated Bleu Scores: {evaluation_metrics['generated_bleu_scores']}"
        )
        logger.info(
            f"Generated Meteor Scores: {evaluation_metrics['generated_meteor_scores']}"
        )

        return evaluation_metrics, evaluation_df

    def evaluate(self):
        if self.args.evaluation_type == "retrieval":
            return self.evaluate_retrieval()
        elif self.args.evaluation_type == "e2e":
            return self.evaluate_e2e()
        else:
            raise ValueError(f"evaluation_type must be either 'retrieval' or 'e2e'")
