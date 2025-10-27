CREATE DATABASE banking_postgres_db
WITH ENGINE = 'postgres',
PARAMETERS = {
    "host": "host.docker.internal",
    "port": 5432,
    "database": "demo",
    "user": "postgresql",
    "password": "psqlpasswd",
    "schema": "demo_data"
};


CREATE ML_ENGINE openai_engine
FROM openai
USING
    openai_api_key = '';

SELECT * FROM information_schema.ml_engines WHERE name = 'openai_engine';

CREATE AGENT classification_agent
USING
    data = {
        "tables": ["banking_postgres_db.conversations_summary"]
    },
    prompt_template= 'You are a banking customer service analyst. Analyze the conversation transcript and provide:

1. A concise summary (2-3 sentences) of the customer interaction
2. Classification of issue resolution status

IMPORTANT GUIDELINES FOR UNRESOLVED:
- If the customer expresses dissatisfaction, frustration, or complaints, mark as UNRESOLVED
- If the agent promises to "pass feedback along" or "escalate" without immediate resolution, mark as UNRESOLVED
- If the customer raises concerns about bank policies, procedures, or communication, mark as UNRESOLVED
- If the conversation ends without clear resolution, mark as UNRESOLVED
- If customer audio is missing or incomplete (e.g., only agent responses visible), mark as UNRESOLVED
- If the agent offers a solution but customer confirmation is missing, mark as UNRESOLVED
- If the conversation is cut off or incomplete, mark as UNRESOLVED

GUIDELINES FOR RESOLVED:
- Only mark as RESOLVED if the customer explicitly confirms satisfaction
- Look for explicit confirmation words like "thank you", "that worked", "issue resolved", "problem solved", "perfect"
- The customer must express contentment with the outcome, not just acceptance

Format your response EXACTLY as:
Summary: [your 2-3 sentence summary describing what happened in the conversation]
Status: [RESOLVED or UNRESOLVED]

Conversation to analyze:',
    timeout = 30;


SELECT answer
FROM test_agent 
WHERE question = '
client: Hi, I\'m calling to inquire about donating to a local charity through Union Financial. Can you help me with that? agent: Ofsolutely, Roseann! I have a few options for charating to charities through our bank. Can you tell me a little bit more about what you\'re looking to? Are you interested in making a one-time donation or setting up a recurring contribution? client: Well, I\'d like to make a one-time donation., but also set up a recurring monthly contributionation as Is that possible? agent: Yes, definitely\'s definitely possible. We me walk you through our process real quick. First, we have a list of pre-approved charities that we work with. Would you like me to send that over to you via email? client: That would be great, thank you! agent: Great. I\'ll send that over right away. Once you\'ve selected the charity you\'d like to don, we can set up the donation. For a one-time donation, we can process that immediately. For the recurring monthly donation, we\'ll need to set up an automatic transfer from your Union Financial account to the charity\'s account. Is that sound good to you? client: Yes, that sounds perfect. How do I go about selecting the charity? agent: Like I mentioned earlier, we have a list of pre-approved charities that we work with. You can review that list and let me know which charity you\'d like to support. If the charity you\'re interested in isn\'t on the list, we can still process the donation, it might take a little longer because we\'ll need to verify some additional information. client: Okay, I see. I think I\'d like to donate to the local animal shelter. They\'re not on the list, but I\'m sure they\'re legitimate. Can we still donate to them? agent: Absolutely! We can definitely still process the donation. the animal shelter. I\'ll just need to collect a bit more information from you to ensure everything goes smoothly. Can you please provide me with the charity\'s name and address? client: Sure! The name of the shelter is PPaws and Claws" and their address is 123 Main Street. agent: Perfect, I\'ve got all the information I need. I\'ll go ahead and process the don-time donation and set up the recurring monthly transfer. Is there anything else I can assist you with today, Roseann? client: No, that\'s all for now. Thank you so much for your help, Guadalupe! agent: You\'re very welcome, Roseann! It was my pleasure to assist you. Just to summary, we\'ve processed up a one-time donation to "aws and Claws Animal Shelter and a recurring monthly transfer to the same organization. Is there anything else I can do you with today? client: Nope, that\'s it! Thanks again! agent: You\'re welcome, Roseann. Have a wonderful day!';


    
SHOW AGENTS;

show SKILLS;
--jiaqi

CREATE DATABASE my_Confluence   --- display name for database.
WITH ENGINE = 'confluence',
PARAMETERS = {
  "api_base": "",
  "username": "",
  "password":""
};

CREATE KNOWLEDGE_BASE my_confluence_kb
USING
    embedding_model = {
        "provider": "openai",
        "model_name": "text-embedding-3-small",
        "api_key":""
    },
    content_columns = ['body_storage_value'],
    id_column = 'id';

DESCRIBE KNOWLEDGE_BASE my_confluence_kb;

INSERT INTO my_confluence_kb (
    SELECT id, title, body_storage_value
    FROM my_confluence.pages
    WHERE id IN ('360449','589825')
);

SELECT COUNT(*) as total_rows FROM my_confluence_kb;

SELECT * FROM my_confluence_kb
WHERE chunk_content = 'Consumer Focus'
LIMIT 3;

CREATE AGENT recommendation_agent
USING
    model = {
        "provider": "openai",
        "model_name": "gpt-4o",
        "api_key":""
    },
    data = {
        "knowledge_bases": ["mindsdb.my_confluence_kb"]
    },
    prompt_template = 'my_confluence_kb stores Confluence pages data, you need to give recommendations according to the customer complaints handling manual';
-- CREATE JOB process_new_conversations (

--     UPDATE banking_postgres_db.conversations_summary
--     SET
--         summary = (
--             SELECT answer
--             FROM classification_agent
--             WHERE question = banking_postgres_db.conversations_summary.conversation_text
--             LIMIT 1
--         ),
--         resolved = CASE
--             WHEN (
--                 SELECT answer
--                 FROM classification_agent
--                 WHERE question = banking_postgres_db.conversations_summary.conversation_text
--                 LIMIT 1
--             ) LIKE '%Status: RESOLVED%' THEN TRUE
--             ELSE FALSE
--         END
--     WHERE
--         banking_postgres_db.conversations_summary.summary IS NULL
-- )
-- EVERY 1 min;


-- show JOBS;
-- DROP JOB process_new_conversations;

-- SHOW TRIGGERS;




-- DROP JOB IF EXISTS fill_conversation_summaries_cache;
-- CREATE JOB fill_conversation_summaries_cache (
--     INSERT INTO demo_data.conversation_summaries_cache (
--         id,
--         conversation_text,
--         summary,
--         resolved
--     )
--     SELECT
--         cs.id,
--         cs.conversation_text,
--         MAX(ca.answer) AS summary,
--         MAX(CASE WHEN ca.answer LIKE '%Status: RESOLVED%' THEN TRUE ELSE FALSE END) AS resolved
--     FROM demo_data.conversations_summary cs
--     JOIN classification_agent ca
--       ON ca.question = cs.conversation_text
--     LEFT JOIN demo_data.conversation_summaries_cache cache
--       ON cache.id = cs.id
--     WHERE cs.summary IS NULL
--       AND cache.id IS NULL
--     GROUP BY cs.id, cs.conversation_text
-- )
-- EVERY 1 MIN;

