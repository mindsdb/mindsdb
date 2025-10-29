# API Reference for Academic Research Copilot

## Overview

The Academic Research Copilot provides a set of APIs to interact with the Knowledge Base of academic papers. This document outlines the available endpoints, their parameters, and usage examples.

## Base URL

```
http://localhost:8501
```

## Endpoints

### 1. Search Papers

- **Endpoint:** `/api/search`
- **Method:** `POST`
- **Description:** Searches for academic papers based on a query string.
- **Request Body:**
  ```json
  {
    "query": "string"
  }
  ```
- **Response:**
  - **200 OK**
    ```json
    {
      "results": [
        {
          "title": "string",
          "authors": ["string"],
          "abstract": "string",
          "url": "string"
        }
      ]
    }
    ```
  - **400 Bad Request**
    ```json
    {
      "error": "string"
    }
    ```

- **Example:**
  ```json
  {
    "query": "machine learning in healthcare"
  }
  ```

### 2. Get Paper Details

- **Endpoint:** `/api/paper/{id}`
- **Method:** `GET`
- **Description:** Retrieves detailed information about a specific academic paper.
- **Path Parameters:**
  - `id` (string): The unique identifier of the paper.
- **Response:**
  - **200 OK**
    ```json
    {
      "title": "string",
      "authors": ["string"],
      "abstract": "string",
      "url": "string",
      "publication_date": "string",
      "keywords": ["string"]
    }
    ```
  - **404 Not Found**
    ```json
    {
      "error": "string"
    }
    ```

- **Example:**
  ```
  GET /api/paper/12345
  ```

### 3. List All Papers

- **Endpoint:** `/api/papers`
- **Method:** `GET`
- **Description:** Retrieves a list of all academic papers in the Knowledge Base.
- **Response:**
  - **200 OK**
    ```json
    {
      "papers": [
        {
          "id": "string",
          "title": "string",
          "authors": ["string"],
          "url": "string"
        }
      ]
    }
    ```

- **Example:**
  ```
  GET /api/papers
  ```

## Error Handling

All API responses include appropriate HTTP status codes and error messages. Common error responses include:

- **400 Bad Request:** The request was invalid or cannot be otherwise served.
- **404 Not Found:** The requested resource could not be found.
- **500 Internal Server Error:** An unexpected error occurred on the server.

## Conclusion

This API reference provides the necessary information to interact with the Academic Research Copilot's Knowledge Base. For further assistance, please refer to the setup and architecture documents.