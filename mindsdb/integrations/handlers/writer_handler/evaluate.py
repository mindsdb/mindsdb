import ast
from collections import defaultdict
from typing import List

import nltk
import pandas as pd
from nltk import word_tokenize
from nltk.translate.bleu_score import (  # todo investigate why this always returns 0, not used for now
    sentence_bleu,
)
from nltk.translate.meteor_score import meteor_score
from rouge_score import rouge_scorer
from scipy.spatial import distance

from mindsdb.integrations.handlers.writer_handler.settings import (
    WriterHandlerParameters,
)
from mindsdb.utilities import log

# todo use polars for this for speed

logger = log.getLogger(__name__)


class WriterEvaluator:
    def __init__(self, args: WriterHandlerParameters, df: pd.DataFrame, rag):

        self.args = args
        self.df = df
        self.rag = rag(self.args)

        self.metric_map = {
            "cosine_similarity": self.calculate_cosine_similarities,
            "accuracy": self.get_matches,
            "rouge": self.calculate_rouge,
            "bleu": self.calculate_bleu,
            "meteor": self.calculate_meteor,
        }

        self.retrieval_metrics = self.args.retrieval_evaluation_metrics
        self.generator_metrics = self.args.generation_evaluation_metrics

        self.mean_evaluation_metrics = defaultdict(list)

        if args.evaluation_type == "e2e":
            # todo check if this is fine for cloud, better to download once and load from disk
            nltk.download("wordnet")

    def calculate_retrieval_metrics(
        self,
        df: pd.DataFrame,
        context_embeddings,
        retrieved_context_embeddings,
        prefix="retrieval_",
    ):
        """Calculate retrieval metrics"""

        for metric in self.retrieval_metrics:
            col_name = f"{prefix}{metric}"
            if metric == "cosine_similarity":
                df[col_name] = self.metric_map[metric](
                    context_embeddings, retrieved_context_embeddings
                )
            elif metric == "accuracy":
                col_name = f"{prefix}match"
                df[col_name] = self.get_matches(
                    gt_embeddings=context_embeddings,
                    test_embeddings=retrieved_context_embeddings,
                    threshold=self.args.retriever_match_threshold,
                )
            else:
                raise ValueError(f"metric {metric} not supported")

            self.store_mean_metric(col_name=col_name, mean_metric=df[col_name].mean())

        return df

    def calculate_generation_metrics(
        self,
        df: pd.DataFrame,
        generated_answer_embeddings,
        reference_answer_embeddings,
        prefix="generator_",
    ):
        """Calculate generation metrics"""

        for metric in self.generator_metrics:
            col_name = f"{prefix}{metric}"
            if metric == "cosine_similarity":

                df[col_name] = self.calculate_cosine_similarities(
                    generated_answer_embeddings, reference_answer_embeddings
                )
            elif metric == "accuracy":
                col_name = f"{prefix}match"
                df[col_name] = self.get_matches(
                    gt_embeddings=reference_answer_embeddings,
                    test_embeddings=generated_answer_embeddings,
                    threshold=self.args.generator_match_threshold,
                )
            elif metric == "rouge":
                df[col_name] = df.apply(
                    lambda x: self.metric_map[metric](
                        x["generated_answers"], x["reference_answers"]
                    ),
                    axis=1,
                )
                self.extract_rogue_scores(df, rogue_scores_col=col_name)
            elif metric == "bleu":
                df[col_name] = df.apply(
                    lambda x: self.metric_map[metric](
                        x["tokenized_generated_answers"],
                        x["tokenized_reference_answers"],
                    ),
                    axis=1,
                )
            elif metric == "meteor":
                df[col_name] = df.apply(
                    lambda x: self.calculate_meteor(
                        x["tokenized_generated_answers"],
                        x["tokenized_reference_answers"],
                    ),
                    axis=1,
                )
            else:
                raise ValueError(f"metric {metric} not supported")

            if metric != "rouge":

                self.store_mean_metric(
                    col_name=col_name, mean_metric=df[col_name].mean()
                )

        return df

    def embed_texts(self, texts: List[str]) -> List[list]:
        """Embed a list of texts"""
        return self.rag.embeddings_model.embed_documents(texts)

    def query_vector_store(self, question: str) -> List:
        """Query the vector store"""
        return self.rag.query_vector_store(question)

    @staticmethod
    def extract_returned_text(vector_store_response: List) -> List:
        # todo: this is a hack, we need to fix this so it works with multiple context ie top_k>1
        # todo handle empty response
        return [doc.page_content for doc in vector_store_response][0]

    def evaluation_prompt(self, question: str, context: str):
        """Create prompt for evaluating RAG"""

        if self.args.summarize_context:

            return self.rag.summarize_context(
                question=question, combined_context=context
            )

        return self.rag.prompt_template.format(question=question, context=context)

    def get_evaluation_prompts(self, df: pd.DataFrame) -> List[str]:
        """Create prompts for each question and context pair in the dataframe"""
        return [
            self.evaluation_prompt(question, context)
            for question, context in zip(df["question"], df["retrieved_context"])
        ]

    def extract_generated_texts(self, responses: List[str]):
        """Extract generated text from LLM response"""

        results = []
        for i, item in enumerate(responses):

            text = self.rag.extract_generated_text(item)

            results.append(text)
        return results

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
        gt_embeddings: List[float], test_embeddings: List[float]
    ) -> float:
        """Calculate cosine similarity between a context and retrieved context embedding"""
        cosine_sim = 1 - distance.cosine(gt_embeddings, test_embeddings)

        return cosine_sim

    def calculate_cosine_similarities(
        self,
        gt_embeddings: List[List[float]],
        test_embeddings: List[List[float]],
    ):
        """Calculate cosine similarity for each ground truth and retrieved/generated pair for a given question"""

        return [
            self._calculate_cosine_similarity(
                context_embedding, retrieved_context_embedding
            )
            for context_embedding, retrieved_context_embedding in zip(
                gt_embeddings, test_embeddings
            )
        ]

    @staticmethod
    def check_match(cosine_similarity: float, threshold: float = 0.7) -> int:
        return int(cosine_similarity >= threshold)

    def get_matches(
        self, gt_embeddings, test_embeddings, threshold: float = 0.7
    ) -> List[int]:
        """Get matches for each ground truth and retrieved/generated pair for a given question"""

        cosine_similarities = self.calculate_cosine_similarities(
            gt_embeddings=gt_embeddings, test_embeddings=test_embeddings
        )

        matches = [
            self.check_match(cosine_similarity, threshold=threshold)
            for cosine_similarity in cosine_similarities
        ]

        return matches

    @staticmethod
    def _tokenize(text: str) -> List[str]:
        """Tokenize a text"""
        return word_tokenize(text)

    @staticmethod
    def calculate_rouge(generated: str, reference: str) -> dict:

        scorer = rouge_scorer.RougeScorer(["rouge1", "rougeL"], use_stemmer=True)
        score = scorer.score(generated, reference)
        return score

    def extract_rogue_scores(
        self, df: pd.DataFrame, rogue_scores_col: str = "rouge_scores"
    ):
        """Extract rouge scores from dataframe"""
        rouge_metrics = ["rouge1", "rougeL"]
        supported_metrics = ["precision", "recall", "fmeasure"]

        for rouge_metric in rouge_metrics:
            for supported_metric in supported_metrics:
                col_name = f"{rouge_metric}_{supported_metric}"
                df[col_name] = df.apply(
                    lambda x: getattr(
                        x[rogue_scores_col][rouge_metric], supported_metric
                    ),
                    axis=1,
                )

                self.store_mean_metric(
                    col_name=col_name, mean_metric=df[col_name].mean()
                )

    def store_mean_metric(self, col_name: str, mean_metric: float):
        """Calculate mean metric for each metric"""

        self.mean_evaluation_metrics[f"mean_{col_name}"].append(mean_metric)

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
            lambda x: self.extract_returned_text(
                self.query_vector_store(x["question"])
            ),
            axis=1,
        )

        # embed context and retrieved context
        context_embeddings = self.embed_texts(df["context"].tolist())
        retrieved_context_embeddings = self.embed_texts(
            df["retrieved_context"].tolist()
        )

        df = self.calculate_retrieval_metrics(
            df, context_embeddings, retrieved_context_embeddings
        )

        return df

    def evaluate_generation(self, df: pd.DataFrame):
        """Evaluate the generation model, given the retrieval results df"""

        prompts = self.get_evaluation_prompts(df)

        raw_generated_answers = [self.rag.llm(prompt) for prompt in prompts]

        generated_answers = self.extract_generated_texts(raw_generated_answers)
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

        df = self.calculate_generation_metrics(
            df, generated_answer_embeddings, reference_answer_embeddings
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
            raise ValueError("evaluation_type must be either 'retrieval' or 'e2e'")
