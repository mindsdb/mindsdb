# Introduction

Want to use generative AI to automate your customer or user interactions? This tutorial shows you step-by-step how to build an interactive Twitter Bot using ChatGPT, MindsDB and MariaDB Enterprise Server in roughly 30-60 minutes. You can follow along with this [tutorial on YouTube](https://www.youtube.com/watch?v=uqn3MLCUhYU) as well. 


# What you’ll build

You’ll build a system that scans for targeted content to respond to, and uses ChatGPT to reply  from a specific bot account live on Twitter like you see here:


<table>
  <tr>
   <td>


<img src="images/Twitter-chatbot-snoopstien.png" width="" alt="alt_text" title="Twitter Bot Profile Page">

<p>
<a href="https://www.twitter.com/snoop_stein">https://www.twitter.com/snoop_stein</a>
   </td>
   <td>




<img src="images/Twitter-chatbot-mindsdb-testing-live-twitter.png" width="" alt="alt_text" title="Live Twitter Bot Interactivity">

   </td>
  </tr>
</table>



# Tutorial Architecture

You’ll use [MindsDB](https://docs.mindsdb.com/what-is-mindsdb) to construct the bot and interact with LLMs like OpenAI’s ChatGPT, while indexing and storing accumulated tweet data in a [MariaDB relational database](https://skysql.cloud/mindsdb-docs). In the diagram below, the teal colored database represents data in MariaDB. The gray/light blue colored database represents data stored in MindsDB. What you build can then be embedded into your application with one of MindsDB SDKs, and/or called from the [MindsDB REST API](https://docs.mindsdb.com/rest/sql).


![alt_text](images/Twitter-chatbot-mindsdb-mariadb-sql.png "Tutorial Architecture")


But wait, isn’t MindsDB a database, you might rightly ask? MindsDB manages AI models and automates workflows connecting AI models with enterprise databases. MindsDB is an AI Database rather than a traditional database. Therefore, if your model necessitates considerable input or yields a substantial output volume, or if you're seeking the data reliability assurances provided by a relational database, it's advisable to allocate that data storage to a different platform. Ultimately, MindsDB focuses on streamlining the processes of linking your data, training models and deploying AI models rather than being a comprehensive storage solution.


# Tutorial Prerequisites 

You’ll need three things to get started: 


1. A [MindsDB account](https://docs.mindsdb.com/quickstart). 
2. A [MariaDB SkySQL account](https://skysql.cloud/mindsdb-docs).
3. A Twitter [developer account](https://developer.twitter.com/en/docs/twitter-api/getting-started/getting-access-to-the-twitter-api). You’ll need your [own API keys /access tokens](https://www.youtube.com/watch?v=qVe7PeC0sUQ) to input parameters like these below into your MindsDB console to build the bot job later in the tutorial.

```
    "consumer_key": "your twitter App API key",  
    "consumer_secret": "your twitter App API key secret",  
    "bearer_token": "your twitter App bearer TOKEN",  
    "access_token": "your twitter App Access Token",  
    "access_token_secret": "your twitter App Access Token Secret"
```

When you are done getting your API keys / tokens for Twitter, you should also have a live Twitter bot profile with a name of your choosing, handle, etc.  You don’t need to go as far as creating a profile image and profile background etc. like this tutorial did for fun - that’s up to you.


## Data Setup

This example uses live data sourced directly from the Twitter API via the [MindsDB Twitter integration](https://mindsdb.com/integrations/twitter), as opposed to an existing data set, CSV file, or other export of some pre-existing database. Accumulated Twitter data will be stored in a Maria DB Enterprise Server Columnstore database deployed on SkySQL, which is designed for operational analytic workloads.


## Creating the MariaDB Enterprise Server database

MariaDB ColumnStore extends MariaDB Enterprise Server database with distributed, columnar storage and a massively parallel processing (MPP) shared nothing architecture, transforming it into a standalone or distributed data warehouse for ad hoc SQL queries and advanced analytics – without the need to create indexes. It’s a great place to store live Twitter data.  Login to SkySQL, and choose “launch a cloud database in SkySQL”.  Then assign these choices, accepting other defaults:



![alt_text](images/animated-gif-skysql-service-create.gif "How to Create SkySQL Service")


**> Technology, topology : Enterprise Server** > **ColumnStore Data Warehouse**

**> Cloud Provider:** Choose as you wish from the available options. Google Cloud works great.  We recommend choosing a nearby region to you, for performance reasons.

**> Instance, Storage, Nodes:** Choose the **Sky 4x16** (4 vCPU, 16GB) instance, which is more than sufficient for learning purposes. SkySQL gives you a free credit to work with but make sure to turn off the instance if you aren’t using it so you don’t waste it.

**> Service Attributes:** Give it a name of your choosing. The tutorial code will reflect the name: **analyticsdb**. 

**> Security:** Click the radio box to ‘**Allow access to the service from specific IPs**’, and then check the box for adding your current IP.  Later, we’ll also add the MindsDB cloud IP addresses.

**> Launch Service**: Lastly, click “**Launch Service**” to create, and wait a few minutes.


## Creating the MariaDB Enterprise Server schema

Now let’s create the schema to hold data from Twitter’s API calls from MindsDB, that will help us identify, track and reply to the right tweets. 

In the SkySQL Console left-hand navigation pane, click **Workspace**> **Query Editor**. Copy and paste the SQL code (below the screenshot) into the editor and click the run button as shown below, which will produce the two tables in the schema browser, `chatbot_input `and `chatbot_output`. 


![alt_text](images/Twitter-chatbot-mindsdb-mariadb-CS-SQL.png "Creating MariaDB datasbase schema")


SQL to execute:

```
CREATE DATABASE chatbotdb;
USE chatbotdb;

SET sql_mode='ANSI_QUOTES';

CREATE TABLE chatbot_input (
  id text CHARACTER SET utf8mb4,
  created_at text CHARACTER SET utf8mb4,
  "text" text CHARACTER SET utf8mb4,
  edit_history_tweet_ids text CHARACTER SET utf8mb4,
  author_id text CHARACTER SET utf8mb4,
  author_name text CHARACTER SET utf8mb4,
  author_username text CHARACTER SET utf8mb4,
  conversation_id text CHARACTER SET utf8mb4,
  in_reply_to_user_id text CHARACTER SET utf8mb4,
  in_reply_to_user_name text CHARACTER SET utf8mb4,
  in_reply_to_user_username text CHARACTER SET utf8mb4,
  in_reply_to_tweet_id text CHARACTER SET utf8mb4,
  in_retweeted_to_tweet_id text CHARACTER SET utf8mb4,
  in_quote_to_tweet_id text CHARACTER SET utf8mb4
  );

 CREATE TABLE chatbot_output (
  id text CHARACTER SET utf8mb4,
  created_at text CHARACTER SET utf8mb4,
  "text" text CHARACTER SET utf8mb4,
  edit_history_tweet_ids text CHARACTER SET utf8mb4,
  author_id text CHARACTER SET utf8mb4,
  author_name text CHARACTER SET utf8mb4,
  author_username text CHARACTER SET utf8mb4,
  conversation_id text CHARACTER SET utf8mb4,
  in_reply_to_user_id text CHARACTER SET utf8mb4,
  in_reply_to_user_name text CHARACTER SET utf8mb4,
  in_reply_to_user_username text CHARACTER SET utf8mb4,
  in_reply_to_tweet_id text CHARACTER SET utf8mb4,
  in_retweeted_to_tweet_id text CHARACTER SET utf8mb4,
  in_quote_to_tweet_id text CHARACTER SET utf8mb4
  );

```

This will create the `chatbot_input `and `chatbot_output `tables. 


# [​](https://docs.mindsdb.com/contribute/tutorials#understanding-the-data)Understanding the Data

Tweets have a long list of fields in the Twitter API and object model; if you look at their [API documentation](https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/tweet) you’ll see this tutorial is using a subset. Of course, the datatypes and field titles are most clearly visible in the SQL script above. You’ll also see them and their values in the MindsDB console (pictured below) as you do testing, develop and execute queries. This will help you target the content you want ChatGPT (another supported LLM) to respond to from Twitter.


![alt_text](images/Twitter-chatbot-mindsdb-console.png "TWitter output in MindsDB console")



# MindsDB Connections Setup


## MindsDB Cloud and MariaDB Enterprise Server

Ok, a little more infrastructure setup to do before we roll up our sleeves and get into the use-case related code. We’ll handle network security first, and then connect MindsDB and MariaDB in the MindsDB SQL Editor.


#### Security

Let’s add the MindsDB Cloud IP addresses to the network allowlist on SkySQL. To get the IPs, click on the **add > new datasource** button in the MindsDB console, and search/choose MariaDB. You’ll see the IP addresses in question on the resulting screen. 

![alt_text](images/Twitter-chatbot-mindsdb-console-new-DS.png "New Datasource Dialog")

![alt_text](images/Twitter-chatbot-mindsdb-sky-allowlist.png "Getting IP addresses for allowlist")


Copy/paste these IPs Into a temporary text document and then add them with the /32 CIDR notation to the SkySQL > **settings**> **secure access** panel as shown below:



![alt_text](images/Twitter-chatbot-mindsdb-mariadb-IP-allow.png  "Configuring allow lists")


If you’re developing on a laptop, MariaDB SkySQL will automatically prompt you to update the allowlist for new IP addresses as you travel, ensuring that your laptop IP can communicate to the query editor, regardless of your location.


#### Declaring a MariaDB Enterprise Server Data Source in MindsDB

The easiest way to work with the MariaDB Enterprise Server is its DBaaS cloud service, SkySQL. In the MindsDB SQL editor, click **add > new datasource** button, and search/choose MariaDB SkySQL. This will open a new query window, and generate a MindsDB SQL template for making the MindsDB &lt;-> MariaDB connection:

```
CREATE DATABASE skysql            --- display name for database.
WITH ENGINE = 'mariadb',
PARAMETERS = {
  "user":" ",                     --- Username associated with database
  "password":" ",                 --- Password to authenticate your access
  "host":" ",                     --- Host to server IP Address or hostname
  "port": "5001",                 --- Port through which TCPIP connection is to be made
  "ssl": True/False,              --- optional, the 'ssl' parameter value indicates whether SSL is enabled ('True') or disabled ('False')
  "ssl-ca": {  "path": " " },     --- Optional, SSL Certificate Authority
  "database":" "                  --- Database name to be connected
};
```

Give the MindsDB SQL query tab a title like ‘TwitterBot’.  

You can find the values to fill in this template in the SkySQL console. To get the values that are specific for your Maria DB Columnstore database instance, simply click 

**Dashboard > Connect** as shown below:


![alt_text](images/mariadb-sky-connect.gif "Accessing SkySQL Connect Dialog")


**About SSL**: We recommend setting SSL to false for development and testing purposes. For production systems, it should be enabled. You can learn more about these settings in the MindsDB [documentation for connecting to MariaDB](https://docs.mindsdb.com/connect/connect-mariadb-skysql).

**About Port**: Note that the default generated port of 5001 should be overridden with the values in the connect dialogue.

**About Database**: if you followed the steps in this tutorial, it would be named <code>[chatbotdb ](https://docs.mindsdb.com/sql/create/model)</code>- this is the name of the MariaDB ColumnStore database you created with the SQL above. 

(analyticsdb is the SkySQL service name).

Then, execute the completed SQL template in the MindsDB SQL editor by highlighting the SQL to execute, and clicking **Run** as shown below:  

(Note that values have been left unmodified in the screenshot).

  

![alt_text](images/Twitter-chatbot-mindsdb-new-DS-SQL.png "Generated SQL template for SkySQL connection")


You’ll notice a DB connection icon now on the left hand nav for MindsDB Demo cloud (mariadb_db in the above screenshot).


## MindsDB Cloud and Twitter

Click the **add > new datasource** button in the MindsDB console, and search/choose Twitter. 



![alt_text](images/Twitter-chatbot-mindsdb-console-new-DS.png "New Datasource Dialog")


[Fill in the template](https://docs.mindsdb.com/app-integrations/twitter) with your Twitter API keys/tokens. Then, in the MindsDB SQL editor, select the SQL, and execute it by clicking **Run**.

```
--- After you fill all the mandatory fields run the query with the top button or shift + enter.
   
CREATE DATABASE my_twitter_bot
With
ENGINE = 'twitter',
PARAMETERS = {
  "bearer_token": " ",                    --- Twitter bearer TOKEN
  'consumer_key': " ",                    --- Twitter consumer key *optional.
  'consumer_secret': " ",                 --- Twitter consumer secret *optional.
  'access_token':" ",                     --- Twitter access token *optional.
  'access_token_secret':" "               --- Twitter access secret token *optional.
};

```

Nice job, you’re done with the setup! 🙌


# [​](https://docs.mindsdb.com/contribute/tutorials#training-a-predictor)Deploying a Model


## Step 1: Creating and Deploying a Model

Here you use the <code>[CREATE MODEL](https://docs.mindsdb.com/sql/create/model)</code> command to create a predictor - for example’s sake, the ‘[Snoopstein](https://twitter.com/snoop_stein)’ model bot. Replace the <code>snoopstein_model </code>with your TwitterBot name, and come up with your [own prompt template](https://www.businessinsider.com/how-to-use-get-better-chatgpt-ai-prompt-guide) that makes sense for your scenario. 

```
CREATE MODEL snoopstein_model
PREDICT response
USING
engine = 'openai',
max_tokens = 300,
temperature = 0.75,
model_name = 'gpt-4', -- you can also use 'text-davinci-003' or 'gpt-3.5-turbo'
api_key = 'your OpenAI api key' -- optional. If unspecified, uses MindsDBs demo key
prompt_template = '
Your are a twitter bot, your name is Snoop Stein (@snoop_stein), and you are helping people with their questions, you are smart and hilarious at the same time.

From input message: {{text}}\
by from_user: {{author_username}}\

In less than 200 characters, write a Twitter response to {{author_username}} in the following format:\
Dear @<from_user>, <respond a rhyme as if you were Snoop Dogg but you also were as smart as Albert Einstein, still explain things like Snoop Dogg would, do not mention that you are part Einstein. Quote from references for further dope reads if it makes sense. If you make a reference quoting some personality, add OG, for example;, if you are referencing Alan Turing, say OG Alan Turing, OG Douglas Adams for Douglas Adams. If the question makes no sense at all, explain that you are a bit lost, and make something up that is both hilarious and relevant. sign with -- SnoopStein by @mindsdb and @mariadb.';
```


After executing, the next step is to check the status of a predictor. If its value is `complete`, you can proceed to the next step.


## Step 2: Testing the Model

Write this code into the SQL editor, select the SQL, and execute it by clicking **Run**.  Replace the author_username with your own personal Twitter handle, for tweet copy that would include an ‘@’ mention. 

```
SELECT response from snoopstein_model
WHERE author_username = "@pieterhumphrey"
AND text = '@snoop_stein, why is gravity so different on the sun?.';
```

# Automating the Workflow with MindsDB Jobs


## Constructing Job Components

You’ll need to complete four job components to monitor Twitter: 



* First, let’s input tweets that need a reply: tweets that mention your bot handle, but are not from the bot, and are not retweets, using standard Twitter search syntax.  
* Then, we input all tweets posted by snoop_stein into the chatbot_output table.  
* Then we create a view that compares the input tweet list to the output tweet list, filtering ones that haven’t been replied to yet into a view.  
* Lastly, you join the model and prepare the replies (using a view).

Note that this is live data – so if there is no content on twitter matching the criteria, nothing gets added to the database. You’ll want to seed some content with the bot account and/or your personal account for this to work. Change the date range to something that works for you, as a way of limiting the result set.  Change the hashtags and ‘@’ mentions to your bot account, snoop_stein has been provided as an example. 

Write and then select the SQL below, and then execute it by clicking **Run**.

```
---- Job 1. Input tweets needing reply to MariaDB chatbot_input table
INSERT INTO skysql.chatbot_input( 
       SELECT *
       FROM my_twitter_bot.tweets
       WHERE
           query = '(@snoopstein OR @snoop_stein OR #snoopstein OR #snoop_stein) -is:retweet -from:snoop_stein'
           AND created_at > '2023-04-04 11:50:00'
)
---- Job 2. Input snoop_stein tweets to MariaDB chatbot_output table
INSERT INTO skysql.chatbot_output (
SELECT * FROM my_twitter_bot.tweets
WHERE query = 'from:snoop_stein'
AND created_at >'2023-04-04 11:50:00'
);

---- Job 3. Filter tweets needing reply into view
CREATE VIEW to_reply_to (
SELECT * FROM skysql.chatbot_input
WHERE conversation_id not in (select r.conversation_id from skysql.chatbot_output as r)
);

---- Job 4. Join View with model and prepare replies
CREATE VIEW to_tweet (
SELECT * FROM to_reply_to
JOIN snoopstein_model
LIMIT 1
);

```

Once you’ve posted some Tweets to match the criteria, let’s make sure it’s working before creating the job. Write and then select each separate SQL statement below one at a time, then execute each by clicking **Run** to ensure data is present.

```
select * from skysql.chatbot_input;
select * from skysql.chatbot_output;
select * from to_reply_to;
select * from to_tweet;
```

## Bringing it all together into a job 

Jobs in MindsDB are similar to cron jobs in UNIX and Linux, running at regularly scheduled intervals. Write and then select the SQL below, and then execute it by clicking **Run**.

```
CREATE JOB chatbot_job (


   -- Part 1
   INSERT INTO skysql.chatbot_input(
       SELECT *
       FROM my_twitter_bot.tweets
       WHERE
           query = '(@snoopstein OR @snoop_stein OR #snoopstein OR #snoop_stein) -is:retweet -from:snoop_stein'
           AND created_at > '2023-04-04 11:50:00'
           AND created_at > "{{PREVIOUS_START_DATETIME}}"        
   );


   -- Part 2
   INSERT INTO skysql.chatbot_output (
       SELECT *
       FROM my_twitter_bot.tweets
       WHERE
       query = 'from:snoop_stein'
       AND created_at > '2023-04-04 11:50:00'
       AND created_at > "{{PREVIOUS_START_DATETIME}}"
   );


   -- Part 3
   INSERT INTO my_twitter_bot.tweets (
       SELECT
           id as in_reply_to_tweet_id,
           response as text
       FROM to_tweet
   )


) EVERY minute;

```

Job frequency at scheduled intervals of &lt; 1 day requires a MindsDB [subscription](https://mindsdb.com/pricing), as the demo cloud allows a minimum job frequency of 1 day without one.


# What’s Next?

Use the MindsDB [JavaScript SDK](https://docs.mindsdb.com/sdk/javascript-sdk) or the [Python SDK](https://docs.mindsdb.com/sdk/python-sdk) to embed what you’ve built in this tutorial into your application code.

While this simple example uses Twitter, MindsDB will soon support Slack and perhaps one day,  Discord. Combined with the ability to <code>[FINETUNE](https://docs.mindsdb.com/sql/api/finetune)</code> existing, trained LLM models with your own data inside a relational database, a future where you can train chatbots to reason about data and/or content you provide doesn’t seem far off. Imagine a support bot that understands your digital assets and knowledge!  

Explore more about how MariaDB Enterprise Server, MariaDB Columnstore, MaxScale Database proxy, and Xpand Distributed SQL can help your projects at [The MariaDB developer hub](https://mariadb.com/developers/), or get in touch with MariaDB experts on our [slack](https://mariadb-community.slack.com/).


