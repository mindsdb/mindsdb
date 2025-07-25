"""
Constants for Salesforce handler.
"""


# TODO: Add the other functions that are supported in the WHERE clause.
def get_handler_instructions(integration_name):
    return f"""
When generating queries for {integration_name}, please follow these guidelines:
- NEVER write reserved keywords (e.g., case, group, order) without backticks. USE FROM {integration_name}.`case` instead of FROM {integration_name}.case.


**FIELD SELECTION:**
- Use exact field names from the data catalog
  CORRECT: SELECT CustomerPriority__c FROM Account
  INCORRECT: SELECT customer_priority FROM Account

**FILTERING (WHERE clause):**
- Special date literals: TODAY, YESTERDAY, LAST_WEEK, LAST_MONTH, LAST_QUARTER, LAST_YEAR, THIS_WEEK, THIS_MONTH, THIS_QUARTER, THIS_YEAR
  CORRECT: WHERE CreatedDate = TODAY
  CORRECT: WHERE LastModifiedDate >= LAST_MONTH
  CORRECT: WHERE CloseDate = THIS_QUARTER
- N-based date literals: LAST_N_DAYS:n, NEXT_N_DAYS:n, LAST_N_WEEKS:n, NEXT_N_WEEKS:n, LAST_N_MONTHS:n, NEXT_N_MONTHS:n
  CORRECT: WHERE CreatedDate >= `LAST_N_DAYS:30`
  CORRECT: WHERE CloseDate <= `NEXT_N_WEEKS:2`
  CORRECT: WHERE ActivityDate >= `LAST_N_MONTHS:6`
  INCORRECT: WHERE CreatedDate >= LAST_N_DAYS:30
  INCORRECT: WHERE CreatedDate >= DATE_SUB(TODAY, INTERVAL 30 DAY)
  INCORRECT: AND LastModifiedDate < CURRENT_DATE() - INTERVAL 30 DAY;
- Date and datetime literals: Use ISO 8601 format (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS.mmm±HH:MM)
  CORRECT: WHERE LastActivityDate = '2025-01-01'
  CORRECT: WHERE CreatedDate <= '2025-01-01T12:45:50.000+00:00'
- LIKE operator: Only supports % wildcard, NO underscore (_) wildcard
  CORRECT: WHERE Name LIKE '%Corp%'
  CORRECT: WHERE Name LIKE 'Acme%'
  INCORRECT: WHERE Name LIKE 'A_me%'
- NOT LIKE operator: NOT supported, use NOT(LIKE) instead
  CORRECT: WHERE NOT(Name LIKE '%Corp%')
  INCORRECT: WHERE Name NOT LIKE '%Corp%'
- RLIKE and REGEXP operators: NOT supported, use LIKE for simple pattern matching instead
  CORRECT: WHERE Name LIKE '%Corp%'
  CORRECT: WHERE Email LIKE '%@example.com'
  INCORRECT: WHERE Name RLIKE '^Acme'
  INCORRECT: WHERE Email REGEXP '[0-9]+'
- BETWEEN operator: NOT supported, use >= AND <= instead
  CORRECT: WHERE CreatedDate >= 2025-01-01 AND CreatedDate <= 2025-12-31
  INCORRECT: WHERE CreatedDate BETWEEN '2025-01-01' AND '2025-12-31'
- EXISTS and NOT EXISTS operators: NOT supported, use IN and NOT IN instead
  CORRECT: WHERE Id IN (SELECT AccountId FROM Contact WHERE Email LIKE '%@example.com')
  CORRECT: WHERE Id NOT IN (SELECT AccountId FROM Contact WHERE Email LIKE '%@example.com')
  INCORRECT: WHERE EXISTS (SELECT Id FROM Contact WHERE Email LIKE '%@example.com')
  INCORRECT: WHERE NOT EXISTS (SELECT Id FROM Contact WHERE Email LIKE '%@example.com')
- Functions: ONLY the following functions are supported in WHERE clauses.
  - CALENDAR_MONTH(), CALENDAR_YEAR(), CALENDAR_QUARTER(), DAY_IN_MONTH(), DAY_IN_WEEK(), DAY_IN_YEAR(), HOUR_IN_DAY(), WEEK_IN_MONTH(), WEEK_IN_YEAR()
    CORRECT: WHERE CALENDAR_YEAR(CreatedDate) = 2025
    CORRECT: WHERE CALENDAR_MONTH(CreatedDate) = 5
    CORRECT: WHERE DAY_IN_WEEK(CreatedDate) = 2

    These above functions cannot be used in the SELECT clause.
    INCORRECT: SELECT CALENDAR_YEAR(CreatedDate) FROM Account
    INCORRECT: SELECT CALENDAR_MONTH(CreatedDate) FROM Account
    INCORRECT: SELECT DAY_IN_WEEK(CreatedDate) FROM Account
    
**COMMON MISTAKES TO AVOID:**
- When filtering by date, always use the special date literals, N-based date literals, or ISO 8601 format.
- Do not use functions like DATE_SUB, CURRENT_DATE(), or DATE_ADD for date calculations.
- N-based date literals must be enclosed in backticks.
"""
