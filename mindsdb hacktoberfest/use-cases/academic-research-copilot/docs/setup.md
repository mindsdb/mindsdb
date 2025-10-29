# Setup Instructions for Academic Research Copilot

This document provides step-by-step instructions for setting up the Academic Research Copilot project. Follow these steps to install dependencies, configure the environment, and run the application.

## Prerequisites

Before you begin, ensure you have the following installed on your machine:

- Python 3.8 or higher
- pip (Python package installer)
- Docker (if you plan to run the application in a container)
- Git (for version control)

## Step 1: Clone the Repository

Clone the project repository from GitHub to your local machine:

```bash
git clone <repository-url>
cd academic-copilot
```

## Step 2: Create a Virtual Environment

It is recommended to create a virtual environment to manage project dependencies:

```bash
python -m venv venv
source venv/bin/activate  # On Windows use `venv\Scripts\activate`
```

## Step 3: Install Dependencies

Install the required Python packages using pip:

```bash
pip install -r requirements.txt
```

## Step 4: Set Up Environment Variables

Copy the example environment file and modify it as needed:

```bash
cp .env.example .env
```

Edit the `.env` file to include your MindsDB and database connection settings.

## Step 5: Set Up the Knowledge Base

Run the setup script to initialize the Knowledge Base:

```bash
bash scripts/setup.sh
```

This script will handle the environment setup and install any necessary libraries.

## Step 6: Populate the Knowledge Base

To populate the Knowledge Base with academic papers, run the following script:

```bash
python scripts/populate_kb.py
```

This will fetch data from the ArXiv API and store it in DuckDB.

## Step 7: Run the Application

You can run the application using the following command:

```bash
python src/app.py
```

Alternatively, if you are using Docker, you can build and run the application with:

```bash
docker-compose up --build
```

## Step 8: Access the UI

Once the application is running, you can access the Streamlit UI by navigating to `http://localhost:8501` in your web browser.

## Step 9: Testing

To run the unit tests for the Knowledge Base manager and query functions, use:

```bash
pytest tests/
```

## Conclusion

You have successfully set up the Academic Research Copilot project. You can now start using the application to search and analyze academic papers. For further information, refer to the other documentation files in the `docs` directory.