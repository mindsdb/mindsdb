# Architecture of the Academic Research Copilot

## Overview

The Academic Research Copilot is designed to facilitate the exploration and analysis of academic papers through a user-friendly interface. The application leverages MindsDB's Knowledge Base capabilities to provide semantic and hybrid search functionalities, enabling users to efficiently retrieve relevant research materials.

## Components

### 1. Application Entry Point
- **File:** `src/app.py`
- **Description:** This is the main entry point of the application. It initializes the application, sets up configurations, and defines the routes for the user interface.

### 2. Configuration
- **File:** `src/config/mindsdb_config.py`
- **Description:** Contains configuration settings for connecting to MindsDB, including database connection parameters and other relevant settings.

### 3. Data Ingestion
- **Files:**
  - `src/data/ingestion.py`
  - `src/data/preprocessing.py`
- **Description:** 
  - `ingestion.py` handles the fetching of academic papers from the ArXiv API and stores their metadata in DuckDB.
  - `preprocessing.py` includes functions for cleaning and formatting the ingested data, ensuring that the metadata is ready for analysis.

### 4. Knowledge Base Management
- **Files:**
  - `src/knowledge_base/kb_manager.py`
  - `src/knowledge_base/queries.py`
- **Description:**
  - `kb_manager.py` manages the creation and population of the MindsDB Knowledge Base, connecting to DuckDB and defining the schema.
  - `queries.py` contains functions for querying the Knowledge Base, allowing users to perform semantic and hybrid searches on the academic papers.

### 5. User Interface
- **Files:**
  - `src/ui/streamlit_app.py`
  - `src/ui/components/search.py`
  - `src/ui/components/results.py`
- **Description:**
  - `streamlit_app.py` sets up the Streamlit web application interface, enabling users to input queries and display results.
  - `search.py` defines the search component, handling user input and query submission.
  - `results.py` defines the results component, displaying search results in a user-friendly format.

### 6. Utilities
- **Files:**
  - `src/utils/logger.py`
  - `src/utils/helpers.py`
- **Description:**
  - `logger.py` contains logging utilities for tracking application events and errors.
  - `helpers.py` includes various helper functions used throughout the application.

## Data Flow

1. **Data Ingestion:** The application fetches academic papers from the ArXiv API using the ingestion module. The metadata is stored in DuckDB.
2. **Data Preprocessing:** The ingested data is cleaned and formatted to ensure consistency and usability.
3. **Knowledge Base Creation:** The cleaned data is used to populate the MindsDB Knowledge Base, which is managed by the kb_manager module.
4. **User Interaction:** Users interact with the application through the Streamlit interface, where they can submit queries.
5. **Query Execution:** The queries are processed using the queries module, retrieving relevant results from the Knowledge Base.
6. **Results Display:** The results are displayed in a user-friendly format, allowing users to explore the academic papers effectively.

## Conclusion

The Academic Research Copilot is structured to provide a seamless experience for users seeking academic research materials. By integrating data ingestion, preprocessing, Knowledge Base management, and a user-friendly interface, the application aims to enhance the research process for academics and researchers alike.