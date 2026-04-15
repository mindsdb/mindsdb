#!/bin/bash

# 1. Delete the old unused test folders
git rm -rf tests/unused/
git rm -f tests/unit/handlers/test_handler_metrics.py
git rm -f mindsdb/utilities/ml_task_queue/tests/test_ml_task_queue.py

# 2. Pluck strictly your core tests from the old branch
git checkout origin/FQE-1952-clean-up-unit-test-v1 -- tests/unit/executor/test_udf.py
git checkout origin/FQE-1952-clean-up-unit-test-v1 -- tests/unit/executor/test_handler_metrics.py
git checkout origin/FQE-1952-clean-up-unit-test-v1 -- tests/unit/utilities/ml_task_queue/test_ml_task_queue.py

# 3. Pluck strictly your handler tests from the old branch (this includes the mlflow fix!)
git checkout origin/FQE-1952-clean-up-unit-test-v1 -- mindsdb/integrations/handlers/apache_doris_handler/tests/test_apache_doris_handler.py
git checkout origin/FQE-1952-clean-up-unit-test-v1 -- mindsdb/integrations/handlers/aqicn_handler/tests/test_aqicn_handler.py
git checkout origin/FQE-1952-clean-up-unit-test-v1 -- mindsdb/integrations/handlers/binance_handler/tests/test_binance_handler.py
git checkout origin/FQE-1952-clean-up-unit-test-v1 -- mindsdb/integrations/handlers/coinbase_handler/tests/test_coinbase_handler.py
git checkout origin/FQE-1952-clean-up-unit-test-v1 -- mindsdb/integrations/handlers/email_handler/tests/test_email_handler.py
git checkout origin/FQE-1952-clean-up-unit-test-v1 -- mindsdb/integrations/handlers/eventbrite_handler/tests/test_eventbrite_handler.py
git checkout origin/FQE-1952-clean-up-unit-test-v1 -- mindsdb/integrations/handlers/hubspot_handler/tests/test_hubspot_handler.py
git checkout origin/FQE-1952-clean-up-unit-test-v1 -- mindsdb/integrations/handlers/instatus_handler/tests/test_instatus_handler.py
git checkout origin/FQE-1952-clean-up-unit-test-v1 -- mindsdb/integrations/handlers/intercom_handler/tests/test_intercom_handler.py
git checkout origin/FQE-1952-clean-up-unit-test-v1 -- mindsdb/integrations/handlers/lightdash_handler/tests/test_lightdash_handler.py
git checkout origin/FQE-1952-clean-up-unit-test-v1 -- mindsdb/integrations/handlers/luma_handler/tests/test_luma_handler.py
git checkout origin/FQE-1952-clean-up-unit-test-v1 -- mindsdb/integrations/handlers/mariadb_handler/tests/test_mariadb_handler.py
git checkout origin/FQE-1952-clean-up-unit-test-v1 -- mindsdb/integrations/handlers/milvus_handler/tests/test_milvus_handler.py
git checkout origin/FQE-1952-clean-up-unit-test-v1 -- mindsdb/integrations/handlers/ms_teams_handler/tests/test_ms_teams_handler.py
git checkout origin/FQE-1952-clean-up-unit-test-v1 -- mindsdb/integrations/handlers/npm_handler/tests/test_npm_handler.py
git checkout origin/FQE-1952-clean-up-unit-test-v1 -- mindsdb/integrations/handlers/oilpriceapi_handler/tests/test_oilpriceapi_handler.py
git checkout origin/FQE-1952-clean-up-unit-test-v1 -- mindsdb/integrations/handlers/paypal_handler/tests/test_paypal_handler.py
git checkout origin/FQE-1952-clean-up-unit-test-v1 -- mindsdb/integrations/handlers/pgvector_handler/tests/test_pgvector_handler.py
git checkout origin/FQE-1952-clean-up-unit-test-v1 -- mindsdb/integrations/handlers/pinecone_handler/tests/test_pinecone_handler.py
git checkout origin/FQE-1952-clean-up-unit-test-v1 -- mindsdb/integrations/handlers/qdrant_handler/tests/test_qdrant_handler.py
git checkout origin/FQE-1952-clean-up-unit-test-v1 -- mindsdb/integrations/handlers/rocket_chat_handler/tests/test_rocket_chat_handler.py
git checkout origin/FQE-1952-clean-up-unit-test-v1 -- mindsdb/integrations/handlers/sap_erp_handler/tests/test_sap_erp_handler.py
git checkout origin/FQE-1952-clean-up-unit-test-v1 -- mindsdb/integrations/handlers/storage_handler/tests/test_storage_handler.py
git checkout origin/FQE-1952-clean-up-unit-test-v1 -- mindsdb/integrations/handlers/symbl_handler/tests/test_symbl_handler.py
git checkout origin/FQE-1952-clean-up-unit-test-v1 -- mindsdb/integrations/handlers/tripadvisor_handler/tests/test_tripadvisor_handler.py
git checkout origin/FQE-1952-clean-up-unit-test-v1 -- mindsdb/integrations/handlers/weaviate_handler/tests/test_weaviate_handler.py
git checkout origin/FQE-1952-clean-up-unit-test-v1 -- mindsdb/integrations/handlers/webz_handler/tests/test_webz_handler.py
git checkout origin/FQE-1952-clean-up-unit-test-v1 -- mindsdb/integrations/handlers/xata_handler/tests/test_xata_handler.py
git checkout origin/FQE-1952-clean-up-unit-test-v1 -- mindsdb/integrations/handlers/zipcodebase_handler/tests/test_zipcodebase_handler.py
git checkout origin/FQE-1952-clean-up-unit-test-v1 -- mindsdb/integrations/handlers/zotero_handler/tests/test_zotero_handler.py
git checkout origin/FQE-1952-clean-up-unit-test-v1 -- mindsdb/integrations/handlers/rag_handler/tests/test_rag_pipelines.py
git checkout origin/FQE-1952-clean-up-unit-test-v1 -- mindsdb/integrations/handlers/rag_handler/tests/test_vectordatabase_dispatch.py
git checkout origin/FQE-1952-clean-up-unit-test-v1 -- mindsdb/integrations/handlers/anthropic_handler/tests/test_anthropic.py
git checkout origin/FQE-1952-clean-up-unit-test-v1 -- mindsdb/integrations/handlers/dspy_handler/tests/test_dspy.py
git checkout origin/FQE-1952-clean-up-unit-test-v1 -- mindsdb/integrations/handlers/google_gemini_handler/tests/test_google_gemini.py
git checkout origin/FQE-1952-clean-up-unit-test-v1 -- mindsdb/integrations/handlers/huggingface_handler/tests/test_huggingface.py
git checkout origin/FQE-1952-clean-up-unit-test-v1 -- mindsdb/integrations/handlers/huggingface_api_handler/tests/test_huggingface_api.py
git checkout origin/FQE-1952-clean-up-unit-test-v1 -- mindsdb/integrations/handlers/langchain_handler/tests/test_langchain.py
git checkout origin/FQE-1952-clean-up-unit-test-v1 -- mindsdb/integrations/handlers/langchain_embedding_handler/tests/test_langchain_embedding.py
git checkout origin/FQE-1952-clean-up-unit-test-v1 -- mindsdb/integrations/handlers/mlflow_handler/tests/test_mlflow.py
git checkout origin/FQE-1952-clean-up-unit-test-v1 -- mindsdb/integrations/handlers/rag_handler/tests/test_rag.py
git checkout origin/FQE-1952-clean-up-unit-test-v1 -- mindsdb/integrations/handlers/time_series_handler/tests/test_time_series_utils.py
git checkout origin/FQE-1952-clean-up-unit-test-v1 -- mindsdb/integrations/handlers/vertex_handler/tests/test_vertex.py
git checkout origin/FQE-1952-clean-up-unit-test-v1 -- mindsdb/integrations/handlers/writer_handler/tests/test_writer.py
