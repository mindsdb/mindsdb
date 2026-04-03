# AG2 Handler

This handler integrates [AG2](https://ag2.ai), an open-source multi-agent framework, with MindsDB. It enables creating and querying multi-agent teams via SQL.

AG2 (formerly AutoGen) has 500K+ monthly PyPI downloads, 4,300+ GitHub stars, and 400+ contributors.

## Setup

### Install dependencies

```bash
pip install "ag2[openai]>=0.11.4,<1.0"
```

### Create an ML engine

```sql
CREATE ML_ENGINE ag2_engine
FROM ag2
USING openai_api_key = 'your-key-here';
```

## Usage

### Create a multi-agent model

```sql
CREATE MODEL research_team
PREDICT answer
USING
    engine = 'ag2_engine',
    agents = '[
        {"name": "Researcher", "system_message": "You research topics and provide key facts with sources."},
        {"name": "Writer", "system_message": "You write clear, engaging summaries from research findings."},
        {"name": "Critic", "system_message": "You review content for accuracy. Say TERMINATE when approved."}
    ]',
    max_rounds = 8,
    speaker_selection = 'auto';
```

### Query the agents

```sql
SELECT answer
FROM research_team
WHERE question = 'What are the main benefits of retrieval-augmented generation?';
```

### Batch queries

```sql
SELECT t.question, m.answer
FROM my_questions AS t
JOIN research_team AS m;
```

## Configuration

### Engine arguments

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `openai_api_key` | Yes | — | API key for the agents' LLM |
| `model` | No | `gpt-4o-mini` | LLM model name |
| `api_type` | No | `openai` | API type (openai, anthropic, etc.) |
| `api_base` | No | — | Custom API base URL |

### Model arguments

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `agents` | No | Single assistant | JSON list of agent definitions |
| `max_rounds` | No | `8` | Max GroupChat rounds |
| `speaker_selection` | No | `auto` | Speaker selection: auto, round_robin, random |
| `mode` | No | `groupchat` | Mode: single or groupchat |

### Agent definition format

```json
[
    {
        "name": "AgentName",
        "system_message": "Agent's role and instructions."
    }
]
```

## Modes

- **single**: One assistant agent handles the query directly
- **groupchat**: Multiple agents collaborate via GroupChat with automatic speaker selection

## Describe

```sql
DESCRIBE MODEL research_team;
DESCRIBE MODEL research_team ATTRIBUTE args;
DESCRIBE MODEL research_team ATTRIBUTE agents;
```
