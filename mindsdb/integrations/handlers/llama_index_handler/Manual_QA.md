## QA Testing LlamaIndex

Testing Llamandex ML framework on Cloud with 3 different webpages. The goal is to create a QA model that can subtract answers from a webpage.
### Test 1 with Blackrock's webpage

**1. Create ML Engine for LlamaIndex**

**Syntax executed:**

`CREATE ML_ENGINE blackrock_qa
FROM llama_index
USING api_key = 'your_api_key';
`

**Results**:

`Query successfully completed`

Make sure that LlamaIndex has an underscore between the two words(llama_index).

**2.Create model for Blackrock**

**Syntax executed:**

`CREATE MODEL qa_blackrock
FROM files
    (SELECT * FROM about_blackrock)
PREDICT Answers
USING 
  engine = 'blackrock_qa', 
  index_class = 'GPTVectorStoreIndex',
  reader = 'DFReader',
  source_url_link = 'https://www.blackrock.com/za/individual/about-us',
  input_column = 'Questions',
  openai_api_key = 'your_api_key';`
  
**3.DESCRIBE MODEL**

**Syntax executed:**

`describe qa_blackrock;`

**Results:**

Error with a blank error message
  `
  
 **4. SELECT statement**

 **Syntax executed:**
 
`SELECT Questions,Answers
FROM mindsdb.qa_blackrock
WHERE Questions = 'What is the best long term investment with minimal risks for private investors';`

Had to run the above query 3 times before it gave an answer.Recieved the error [blackrock_qa/qa_blackrock]: [Errno 2] No such file or directory: '/home/ubuntu/.local/share/mindsdb/var/content/predictor/predictor_689_24607/docstore.json'

**Syntax executed:**

`SELECT Questions,Answers
FROM mindsdb.qa_blackrock
WHERE Questions = 'When was Blackrock founded?';`

**Results:**

Blackrock was founded in 1988.

**Syntax executed:**

`SELECT Questions,Answers
FROM mindsdb.qa_blackrock
WHERE Questions = 'Why should investors choose Blackrock?';`

**Results:**

`Investors should choose Blackrock because it is a trusted and reliable leader in the investment management industry. Blackrock has a long track record of successful investments and offers a wide range of services and products to meet the needs of its customers. Additionally, Blackrock has a strong commitment to corporate responsibility and responsible investing, making it an ideal choice for investors who value sustainability.
`

**Syntax executed:**

`SELECT Questions,Answers
FROM mindsdb.qa_blackrock
WHERE Questions = 'Which funds focus on environmental stability';`

Tried 3 times before getting an answer.

**Results:**

`The answer to this question depends on the type of investor and their risk tolerance. There are funds that focus on environmental stability, such as sustainable and socially responsible investments (SRI). These funds often have a focus on renewable energy, clean water, and other environmental sustainability initiatives. Additionally, many investors may opt for exchange-traded funds (ETFs) that track the performance of companies that are actively promoting environmental sustainability.`

**Syntax executed:**

`SELECT Questions,Answers
FROM mindsdb.qa_blackrock
WHERE Questions = 'How many offices does Blackrock have?';`

>Site advises it has 70 offices.

**Results:**

`It is not possible to answer this question with the provided context information.`

`SELECT Questions,Answers
FROM mindsdb.qa_blackrock
WHERE Questions = 'Who owns Blackrock?';`

This is a question and paragraph on the website.

**Results:**

`There is not enough information provided to answer this question. `


**5.Join batch prediction**

`SELECT a.Questions,b.Answers
FROM mindsdb.qa_blackrock as b
JOIN files.about_blackrock as a;`

**Results:**

First try- received an error:error in apply predictor step: [blackrock_qa/qa_blackrock]: [Errno 2] No such file or directory: '/home/ubuntu/.local/share/mindsdb/var/content/predictor/predictor_689_24607/docstore.json'

By the third try the processing time was long:
`error in apply predictor step: [blackrock_qa/qa_blackrock]: RetryError[<Future at 0x7f311c321460 state=finished raised RateLimitError>]`

Removed all the questions that were giving errors and retrained the predictor.

**Syntax executed:**

`SELECT a.Questions, b.Answers
FROM mindsdb.qa_blackrock as b
JOIN files.about_blackrock as a;`

**Results:**

`error in apply predictor step: [blackrock_qa/qa_blackrock]: [Errno 2] No such file or directory: '/home/ubuntu/.local/share/mindsdb/var/content/predictor/predictor_689_24612/docstore.json'`

Tested selecting one answer after the retrain:

**Syntax executed:**

`SELECT Questions,Answers
FROM mindsdb.qa_blackrock
WHERE Questions = 'Which funds focus on environmental stability?';`

The above syntax previously had to be executed 3 times before giving results.

**Results:**

`
Answer: It is not possible to answer this question without prior knowledge.
`

**Results:**

`error in apply predictor step: [blackrock_qa/qa_blackrock]: RetryError[<Future at 0x7fc3787723a0 state=finished raised RateLimitError>]`

Unable to run a batch prediction.

Narrowed down the dataset to 3 questions that has previously given results,uploaded the file and retrained the predictor.

Retried the SELECT statement

**Syntax executed:**

`SELECT Questions,Answers
FROM mindsdb.qa_blackrock
WHERE Questions = 'When was Blackrock founded?';`

**Results:**

`[blackrock_qa/qa_blackrock]: [Errno 2] No such file or directory: '/home/ubuntu/.local/share/mindsdb/var/content/predictor/predictor_689_24613/docstore.json'`

and

`[blackrock_qa/qa_blackrock]: RetryError[<Future at 0x7fc2a6f51e20 state=finished raised RateLimitError>]`

**Conclusion:** Blackrock webpage https://www.blackrock.com/za/individual/about-us
The model is inconsistant with providing results. It will produce an error,and after running the query multiple times it will produce valid answers. Due to this, the model is unable to run batch predictions.One question was a header with a drop down that had text on the webpage, but the model could still not provide the answer, only 2 questions were provided answers after multiple tries.Describe model gives an error with a blank error message.


### Testing 2 with Nordea's webpage

**1. Create ML Engine**

`CREATE ML_ENGINE llama_index
FROM llama_index
USING api_key = 'your_api_key';`

2. Upload file.

**3. Create model**

**Syntax executed:**

`CREATE MODEL nordea_qa
FROM files
    (SELECT * FROM about_nordea)
PREDICT answer
USING 
  engine = 'llama_index', 
  index_class = 'GPTVectorStoreIndex',
  reader = 'DFReader',
  source_url_link = 'https://www.nordea.lu/en/professional/fund/global-stable-equities/',
  input_column = 'question',
  openai_api_key = 'your_api_key';
  `
  
**Results:**
  
  Model status shows completed, no errors.

**4. DESCRIBE MODEL**

**Syntax executed:**

`describe nordea_qa;`

**Results:**
Error with a blank error message
  
5.SELECT statements

**Syntax executed:**

`SELECT question,answer
FROM mindsdb.nordea_qa
WHERE question = 'What is Nordea?';` 

**Results:**

`Nordea is a Nordic banking group operating in Northern Europe with more than 11 million customers. It provides banking services such as savings, investments, loans, payments, and insurance.`

**Syntax executed:**

`SELECT question,answer
FROM mindsdb.nordea_qa
WHERE question = 'How does Nordea manage your assets?';`

Results:

`[llama_index/nordea_qa]: [Errno 2] No such file or directory: '/home/ubuntu/.local/share/mindsdb/var/content/predictor/predictor_689_24619/docstore.json`

Ran the query for the 2nd time, Results:
`Nordea offers a range of asset management services to help customers meet their financial goals. They provide tailored advice and use a variety of investment strategies such as diversification, risk management, and active portfolio management to help customers manage their assets. They also offer a range of products and services to help customers save for retirement and other long-term financial goals.`

**Syntax executed:**

`SELECT question,answer
FROM mindsdb.nordea_qa
WHERE question = 'What does Nordea offer?';`

Results on 1st try:
`[llama_index/nordea_qa]: [Errno 2] No such file or directory: '/home/ubuntu/.local/share/mindsdb/var/content/predictor/predictor_689_24619/docstore.json'`

3rd try: `Answer: Nordea offers financial services, including banking, investments, life insurance, and pensions.`

**Syntax executed:**

`SELECT question,answer
FROM mindsdb.nordea_qa
WHERE question = 'Will investors be able to recover their full amount invested?';`

Results:

1st try:`Answer: It is not possible to answer this question without prior knowledge.`

2nd try:`[llama_index/nordea_qa]: [Errno 2] No such file or directory: '/home/ubuntu/.local/share/mindsdb/var/content/predictor/predictor_689_24619/docstore.json`

3rd try:

`It depends. There are no guarantees that investors will be able to recover their full amount invested. Depending on the type of investment and market conditions, investors can potentially lose money. `

**Syntax executed:**

`SELECT question,answer
FROM mindsdb.nordea_qa
WHERE question = 'Who is the Nordea Stable Equities / Multi Assets Team?';`

3rd try: `The Nordea Stable Equities / Multi Assets Team is a group of experienced asset managers who specialize in providing diversified portfolios and long-term stability. They focus on a wide range of asset classes, including equities, fixed income, real estate, and alternative investments.`

**6.Batch Prediction**

**Syntax executed:**

`SELECT a.question,b.answer
FROM mindsdb.nordea_qa as b
JOIN files.about_nordea as a;`

I predicted with every question in the dataset to see if the model will become familiar with the answers and give results with the batch prediction.

**Results:**
`error in apply predictor step: [llama_index/nordea_qa]: [Errno 2] No such file or directory: '/home/ubuntu/.local/share/mindsdb/var/content/predictor/predictor_689_24619/docstore.json'`

2nd try:`error in apply predictor step: [llama_index/nordea_qa]: RetryError[<Future at 0x7f2aac0c88e0 state=finished raised RateLimitError>]`

**Conclusion:**
With this webpage only one question gave an answer at the first try. The other questions had to be run multiple times but the questions were answered. Not able to do batch predictions.Describe model gives an error with a blank error message.

### Test 3 with Global Investment's webpage

**1. Create model**

**Syntax executed:**

`CREATE MODEL global_investment_qa
FROM files
    (SELECT * FROM global_investment)
PREDICT answer
USING 
  engine = 'llama_index', 
  index_class = 'GPTVectorStoreIndex',
  reader = 'DFReader',
  source_url_link = 'https://globalinvestsinc.com/detroit-investment-guide/',
  input_column = 'question',
  openai_api_key = 'your_api_key';`
  
**Results:**

On the 1st execution, received error: `RetryError: RetryError[], raised at: /usr/local/lib/python3.8/dist-packages/mindsdb/integrations/libs/learn_process.py#104`

On the 2nd execution the model trained with a complete status.
  
  
**2. Describe model**

**Syntax executed:**

`global_investment_qa`

**Results:**

Error with a blank error message.

**3. SELECT statement**

**Syntax executed:**
`
SELECT question,answer
FROM mindsdb.global_investment_qa
WHERE question = 'Why should investors invest into Detroit?';`

**Results:**

`[llama_index/global_investment_qa]: [Errno 2] No such file or directory: '/home/ubuntu/.local/share/mindsdb/var/content/predictor/predictor_689_24621/docstore.json'

**Syntax executed:**

`SELECT question,answer
FROM mindsdb.global_investment_qa
WHERE question = 'What is the rental market in Detroit?';`

**Results:**

The answer executed on the first try.

`The rental market in Detroit has seen steady growth over the past few years, with an increasing number of people opting to rent instead of buy in the city. Average rental rates remain lower than other major cities in the region, making Detroit a great investment opportunity for those looking for a property that will generate steady returns. Additionally, the city's population growth, job market expansion, and strong infrastructure make it a desirable location for renters.`

**Syntax executed:**

`SELECT question,answer
FROM mindsdb.global_investment_qa
WHERE question = 'What are the growth and returns?';`

**Results:**

`[llama_index/global_investment_qa]: RetryError[<Future at 0x7f2a62836490 state=finished raised RateLimitError>]`

AND

`[llama_index/global_investment_qa]: [Errno 2] No such file or directory: '/home/ubuntu/.local/share/mindsdb/var/content/predictor/predictor_689_24621/docstore.json'`

**Syntax executed:**

`SELECT question,answer
FROM mindsdb.global_investment_qa
WHERE question = 'What companies are investing in Detroit?';`

The above question is straight forward and is part of the first paragraph on the site,it clearly lists the answers provided on the webpage.

**Results:**

On the 5th try:`
`[llama_index/global_investment_qa]: RetryError[<Future at 0x7f2af033d820 state=finished raised RateLimitError>]
`

On the **8th time** the query was executed:

`Some of the companies investing in Detroit include Quicken Loans, General Motors, JPMorgan Chase, Ford Motor Company, and DTE Energy.`

**4. Batch predictions**

**Syntax executed:**

`SELECT a.question,b.answer
FROM files.global_investment as a 
JOIN mindsdb.global_investment_qa as b;`

**Results:**

`error in apply predictor step: [llama_index/global_investment_qa]: [Errno 2] No such file or directory: '/home/ubuntu/.local/share/mindsdb/var/content/predictor/predictor_689_24621/docstore.json'`

### Full Conclusion:
- Models with LlamaIndex engines most of the time do not produce answers from information that is within a drop down on first execution, this is an issue as most information these days are categorized and put in drop downs to either a title or question. 
- Queries have to be executed multpiple times as it produces error messsages before an answer is provided,some queries only provides error messages.
- The 2 common error messages are `[llama_index/global_investment_qa]: [Errno 2] No such file or directory: '/home/ubuntu/.local/share/mindsdb/var/content/predictor/predictor_689_24621/docstore.json'` and `[llama_index/global_investment_qa]: RetryError[<Future at 0x7f2af033d820 state=finished raised RateLimitError>]`, or the answer column will produce a message `Answer: It is not possible to answer this question without prior knowledge.`
- After executing multiple times, the answer is provided. However, if you run the SELECT statement that was successfull in providing an answer and run it again, it will provide an error message. 
- None of the batch predictions succesfully executed.
