#!/usr/bin/env python3
"""
ArXiv paper processing pipeline script.

This module downloads new ArXiv papers from yesterday, processes them through
the pipeline, and manages metadata files.
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import List, Dict, Any

from paperscraper.get_dumps import arxiv, biorxiv, medrxiv, chemrxiv

from src import config_loader, psql
from src.arxiv_pipeline import ArxivProcessPipeline
from src.MindsDBMiddleware import knowledge_base, manager

# Constants
METADATA_FILE_PATH = "./new_paper_metadata.json"
CONFIG_FILE_PATH = None
LOG_FILE_PATH = "cron_arxiv_processor.log"

# Configure logging
logging.basicConfig(
    level=config_loader.app.log_level,
    format=config_loader.app.log_format,
    handlers=[logging.FileHandler("logs/cron.log"), logging.StreamHandler()],
)

logger = logging.getLogger(__name__)

mdb = manager.MindsDBManager()
psql_client = psql.PostgresHandler()
kb = knowledge_base.KnowledgeBase(mdb)


def download_new_arxiv_ids() -> None:
    """
    Download ArXiv paper metadata for yesterday's papers.

    Raises:
        Exception: If the download fails.
    """
    try:
        yesterday = datetime.now() - timedelta(days=1)
        formatted_date = yesterday.strftime("%Y-%m-%d")

        logger.info(f"Starting download of ArXiv papers for date: {formatted_date}")
        arxiv(start_date=formatted_date, end_date=None, save_path=METADATA_FILE_PATH)
        logger.info(f"Successfully downloaded ArXiv metadata to {METADATA_FILE_PATH}")

    except Exception as e:
        logger.error(f"Failed to download ArXiv papers: {e}")
        raise


def read_downloaded_metadata() -> List[Dict[str, Any]]:
    """
    Read and parse the downloaded metadata file.

    Returns:
        List of paper metadata dictionaries.

    Raises:
        FileNotFoundError: If the metadata file doesn't exist.
        Exception: If there's an error reading the file.
    """
    if not os.path.exists(METADATA_FILE_PATH):
        logger.error(f"Metadata file not found: {METADATA_FILE_PATH}")
        raise FileNotFoundError(f"Metadata file not found: {METADATA_FILE_PATH}")

    papers_metadata = []
    failed_papers = 0

    try:
        logger.info(f"Reading metadata from {METADATA_FILE_PATH}")

        with open(METADATA_FILE_PATH, "r", encoding="utf-8") as f:
            for line_num, paper_line in enumerate(f, 1):
                try:
                    paper_data = json.loads(paper_line.strip())
                    papers_metadata.append(paper_data)
                except json.JSONDecodeError as e:
                    failed_papers += 1
                    logger.warning(
                        f"Failed to parse JSON on line {line_num}: {e}. "
                        f"Line content: {paper_line[:100]}..."
                    )
                except Exception as e:
                    failed_papers += 1
                    logger.warning(f"Unexpected error on line {line_num}: {e}")

        logger.info(
            f"Successfully read {len(papers_metadata)} papers. "
            f"Failed to parse {failed_papers} papers."
        )

    except Exception as e:
        logger.error(f"Error reading metadata file: {e}")
        raise

    return papers_metadata


def process_new_arxiv_ids() -> None:
    """
    Process all downloaded ArXiv papers through the pipeline.

    Raises:
        Exception: If initialization of services fails.
    """
    try:
        papers_metadata = read_downloaded_metadata()

        if not papers_metadata:
            logger.warning("No papers to process")
            return

        logger.info(f"Starting processing of {len(papers_metadata)} papers")

        processed_count = 0
        failed_count = 0

        for i, paper_metadata in enumerate(papers_metadata, 1):
            try:
                # Extract ArXiv ID from DOI
                doi = paper_metadata.get("doi", "")
                if not doi:
                    logger.warning(f"Paper {i}: Missing DOI, skipping")
                    failed_count += 1
                    continue

                doi_parts = doi.split("/")
                if len(doi_parts) < 2:
                    logger.warning(f"Paper {i}: Invalid DOI format '{doi}', skipping")
                    failed_count += 1
                    continue

                arxiv_id = doi_parts[1].replace("arXiv.", "")

                logger.info(f"Processing paper {i}/{len(papers_metadata)}: {arxiv_id}")

                # Process paper through pipeline
                pipeline = ArxivProcessPipeline(
                    arxiv_id=arxiv_id, knowledge_base=kb, postgres_client=psql_client
                )
                pipeline.process(create_paper_kb=False, add_to_main_kb=False)

                processed_count += 1
                logger.info(f"Successfully processed paper: {arxiv_id}")

            except Exception as e:
                failed_count += 1
                arxiv_id = paper_metadata.get("doi", "unknown")
                logger.error(f"Failed to process paper {arxiv_id}: {e}")

        logger.info(
            f"Processing complete. Successfully processed: {processed_count}, "
            f"Failed: {failed_count}"
        )

    except Exception as e:
        logger.error(f"Error during paper processing: {e}")
        raise


def remove_file() -> None:
    """
    Remove the metadata file after processing.
    """
    try:
        if os.path.exists(METADATA_FILE_PATH):
            os.remove(METADATA_FILE_PATH)
            logger.info(f"Successfully removed metadata file: {METADATA_FILE_PATH}")
        else:
            logger.info(f"Metadata file not found for removal: {METADATA_FILE_PATH}")
    except Exception as e:
        logger.error(f"Failed to remove metadata file: {e}")


def create_mdb_job():
    try:
        job_name = "paperscrape_cron"
        if job_name not in mdb.client.jobs.list():
            columns = ", ".join(
                set(
                    config_loader.kb.content_columns + config_loader.kb.metadata_columns
                )
            )
            job_query = f"""
                INSERT INTO {config_loader.kb.name} (
                    {columns}
                    ) 
                    SELECT 
                    {columns}
                    FROM 
                    (
                        SELECT 
                        * 
                        FROM 
                        {config_loader.psql.database}.public.{config_loader.psql.table_name} 
                        WHERE 
                        id > LAST
                );
            """
            interval = "1 day"
            logger.info(
                f"{job_name} not found. Creating a new job with query - {job_query} and interval = {interval}"
            )
            mdb.client.jobs.create(
                name=job_name, query_str=job_query, repeat_str=interval
            )
            logger.info(f"{job_name} job created successfully")
    except Exception as e:
        logger.error(f"Failed to create a job: {e}")
        raise


def start_arxiv_pipeline() -> None:
    """
    Main execution function that orchestrates the entire pipeline.
    """
    if CONFIG_FILE_PATH is not None:
        config_loader.set_config(CONFIG_FILE_PATH)

    logger.info("Starting ArXiv processing pipeline")

    try:
        # Create a job if not exists
        create_mdb_job()

        # Download new ArXiv papers
        download_new_arxiv_ids()

        # Process the downloaded papers
        process_new_arxiv_ids()

        # Clean up the metadata file
        remove_file()

        logger.info("ArXiv processing pipeline completed successfully")

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        # Attempt cleanup even on failure
        try:
            remove_file()
        except Exception as cleanup_error:
            logger.error(f"Cleanup also failed: {cleanup_error}")

        sys.exit(1)

def evaluate_kb():
    evaluate_query = f"""
        EVALUATE KNOWLEDGE_BASE {config_loader.kb.name}
        USING
            test_table = {config_loader.psql.database}.test_{config_loader.psql.table_name},
            version = 'llm_relevancy',
            generate_data = {
                'count': 100
            }, 
            evaluate = true,
            llm = {{
                'provider': 'openai',
                'api_key': '{config_loader.app.openai_api_key}',
                'model':'gpt-4'
            }},
            save_to = {config_loader.psql.database}.evaluation_results;
    """
    drop_test_table = """
        DROP TABLE {config_loader.psql.database}.test_{config_loader.psql.table_name}
    """
    try:
        logging.info("starting evaluation job")
        mdb.execute_query(evaluate_query)
        mdb.execute_query(drop_test_table)
        logging.info("evaluation job complete")
    except Exception as e:
        logger.error(f"Failed to evaluate the knowledge base: {e}")
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="MindsDB Benchmark and Stress Testing Suit"
    )

    parser.add_argument(
        "--arxiv", default=False, action="store_true", help="Run Arxiv Pipeline downloading new papers"
    )
    parser.add_argument(
        "--evaluate_kb", default=False, action="store_true", help="Run evaluation of the knowledge base"
    )

    args = parser.parse_args()

    if args.path:
        config_loader.set_config(args.path)

    if args.arxiv:
        start_arxiv_pipeline()

    if args.evaluate_kb: 
        evaluate_kb()