---
title: Freshdesk
sidebarTitle: Freshdesk
---

This documentation describes the integration of MindsDB with [Freshdesk](https://www.freshdesk.com/), which provides customer support ticketing software for businesses.

The integration allows MindsDB to access data from Freshdesk and enhance it with AI capabilities for ticket analysis, sentiment analysis, and intelligent querying of support data.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](https://docs.mindsdb.com/setup/self-hosted/docker) or [Docker Desktop](https://docs.mindsdb.com/setup/self-hosted/docker-desktop).
2. To connect Freshdesk to MindsDB, install the required dependencies following [this instruction](https://docs.mindsdb.com/setup/self-hosted/docker#install-dependencies).
3. Have access to a Freshdesk account with API access enabled.

## Connection

Establish a connection to Freshdesk from MindsDB by executing the following SQL command and providing its handler name as an engine.

```sql
CREATE DATABASE freshdesk_datasource
WITH
    ENGINE = 'freshdesk',
    PARAMETERS = {
      "api_key": "your_api_key_here",
      "domain": "yourcompany.freshdesk.com"
    };
```

Required connection parameters include the following:

* `api_key`: The API key for your Freshdesk account.
* `domain`: Your Freshdesk domain (e.g., 'yourcompany.freshdesk.com').

<Tip>
To generate an API key in Freshdesk:
1. Log in to your Freshdesk account
2. Click on your profile picture in the top right corner
3. Go to Profile Settings
4. Your API key will be available in the right sidebar under "Your API Key"
</Tip>

## Usage

Retrieve data from a specified table by providing the integration and table names:

```sql
SELECT *
FROM freshdesk_datasource.table_name
LIMIT 10;
```

### Query Examples

#### Get all tickets
```sql
SELECT *
FROM freshdesk_datasource.tickets
LIMIT 50;
```

#### Get tickets by status
```sql
SELECT id, subject, status, priority, created_at
FROM freshdesk_datasource.tickets
WHERE status = 'Open'
ORDER BY created_at DESC;
```

#### Get high priority tickets
```sql
SELECT *
FROM freshdesk_datasource.tickets
WHERE priority = 4  -- High priority
AND status IN ('Open', 'Pending');
```

#### Get agent information
```sql
SELECT id, name, email, active, job_title
FROM freshdesk_datasource.agents
WHERE active = true;
```

#### Complex query: High-risk escalation tickets
```sql
SELECT t.id, t.subject, t.status, t.priority, t.created_at, 
       a.name as agent_name, a.email as agent_email
FROM freshdesk_datasource.tickets t
JOIN freshdesk_datasource.agents a ON t.responder_id = a.id
WHERE t.priority >= 3  -- High or Urgent priority
AND t.status = 'Open'
AND t.fr_escalated = true
ORDER BY t.created_at ASC;
```

<Note>
The above examples utilize `freshdesk_datasource` as the datasource name, which is defined in the `CREATE DATABASE` command.
</Note>

## Supported Tables

The Freshdesk integration supports the following tables:

### `tickets`
Contains all ticket data from your Freshdesk account.

**Key columns:**
- `id`: Unique ticket identifier
- `subject`: Ticket subject
- `description`: Full ticket description
- `status`: Ticket status (Open, Pending, Resolved, Closed)
- `priority`: Priority level (1=Low, 2=Medium, 3=High, 4=Urgent)
- `requester_id`: ID of the customer who created the ticket
- `responder_id`: ID of the agent assigned to the ticket
- `group_id`: ID of the group handling the ticket
- `created_at`: Ticket creation timestamp
- `updated_at`: Last update timestamp
- `due_by`: Due date for ticket resolution
- `sentiment_score`: AI-generated sentiment score
- `satisfaction_rating`: Customer satisfaction rating
- `tags`: Ticket tags

### `agents`
Contains information about all agents in your Freshdesk account.

**Key columns:**
- `id`: Unique agent identifier
- `name`: Agent's full name
- `email`: Agent's email address
- `active`: Whether the agent is active
- `job_title`: Agent's job title
- `department_ids`: Departments the agent belongs to
- `group_ids`: Groups the agent is part of
- `role_ids`: Agent's roles
- `created_at`: When the agent was added
- `last_active_at`: Last activity timestamp
- `ticket_scope`: Scope of tickets the agent can see

## Use Cases

### 1. Ticket Analysis and Prioritization
Use AI to analyze ticket content and automatically prioritize based on urgency indicators:

```sql
-- Create a model to predict ticket priority
CREATE MODEL ticket_priority_predictor
FROM freshdesk_datasource
(SELECT subject, description, type, source, priority FROM tickets WHERE priority IS NOT NULL)
PREDICT priority;

-- Use the model to predict priority for new tickets
SELECT id, subject, predicted_priority
FROM freshdesk_datasource.tickets t
JOIN mindsdb.ticket_priority_predictor m
WHERE t.priority IS NULL;
```

### 2. Sentiment Analysis
Analyze customer sentiment from ticket descriptions:

```sql
-- Create a sentiment analysis model
CREATE MODEL ticket_sentiment_analyzer
PREDICT sentiment
USING
  engine = 'huggingface',
  task = 'text-classification',
  model_name = 'cardiffnlp/twitter-roberta-base-sentiment-latest',
  input_column = 'description';

-- Analyze sentiment of recent tickets
SELECT t.id, t.subject, m.sentiment, m.sentiment_confidence
FROM freshdesk_datasource.tickets t
JOIN mindsdb.ticket_sentiment_analyzer m
WHERE t.created_at > '2024-01-01';
```

### 3. Agent Workload Analysis
Identify agents with high workloads or potential burnout:

```sql
-- Get agent workload statistics
SELECT 
    a.id, a.name, a.email,
    COUNT(t.id) as open_tickets,
    AVG(CASE WHEN t.priority = 4 THEN 1 ELSE 0 END) as urgent_ticket_ratio
FROM freshdesk_datasource.agents a
LEFT JOIN freshdesk_datasource.tickets t ON a.id = t.responder_id
WHERE t.status IN ('Open', 'Pending')
GROUP BY a.id, a.name, a.email
ORDER BY open_tickets DESC;
```

### 4. Escalation Risk Detection
Identify tickets at high risk of escalation:

```sql
-- Find tickets that might escalate
SELECT id, subject, status, priority, created_at, 
       DATEDIFF(NOW(), created_at) as days_open
FROM freshdesk_datasource.tickets
WHERE status = 'Open'
AND priority >= 3
AND DATEDIFF(NOW(), created_at) > 2
AND fr_escalated = false
ORDER BY days_open DESC;
```

## Integration with Model Context Protocol (MCP)

This Freshdesk integration is designed to work seamlessly with MCP (Model Context Protocol), allowing AI assistants like Cursor to intelligently query and analyze your Freshdesk data.

Example MCP interaction:
> "Show me all high priority tickets assigned to agents in the support team that have been open for more than 3 days"

The AI can automatically construct the appropriate SQL query and provide insights based on the results.

## API Rate Limits

Freshdesk has API rate limits that vary by plan:
- **Estate**: 1000 API calls per hour
- **Forest**: 1000 API calls per hour  
- **Garden**: 1000 API calls per hour
- **Blossom**: 1000 API calls per hour

The integration handles pagination automatically and respects these limits.

## Troubleshooting

### Common Issues

1. **Authentication Error**: Verify your API key is correct and hasn't expired
2. **Domain Not Found**: Ensure your domain is correctly formatted (e.g., 'company.freshdesk.com')
3. **Rate Limit Exceeded**: Wait for the rate limit window to reset or upgrade your plan

### Error Messages

- `Authentication failed`: Check your API key
- `Domain not found`: Verify your Freshdesk domain
- `Connection failed`: Check your internet connection and domain accessibility

## Limitations

- The integration currently supports read-only operations (SELECT queries)
- Some advanced Freshdesk features may not be fully supported
- Large datasets may require pagination and could take time to retrieve

## Future Enhancements

Planned features include:
- Write operations (INSERT, UPDATE) for creating and updating tickets
- Support for Freshdesk conversations and notes
- Advanced filtering and search capabilities
- Real-time webhook integration for live data updates
