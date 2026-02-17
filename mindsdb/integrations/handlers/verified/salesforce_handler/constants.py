"""
Constants for Salesforce handler.
"""


def get_soql_instructions(integration_name):
    return f"""This handler executes SOQL (Salesforce Object Query Language), NOT SQL! Follow these rules strictly:

**BASIC STRUCTURE:**
- NO "SELECT *" - must explicitly list all fields
  SQL: SELECT * FROM Account;
  SOQL: SELECT Id, Name, Industry FROM Account
- NO table aliases - use full table names only
  SQL: SELECT a.Name FROM Account a;
  SOQL: SELECT Name FROM Account
- NO column aliases - field names cannot be aliased
  SQL: SELECT Name AS CompanyName FROM Account;
  SOQL: SELECT Name FROM Account
- NO DISTINCT keyword - not supported in SOQL
  SQL: SELECT DISTINCT Industry FROM Account;
  SOQL: Not possible - use separate logic
- NO subqueries in FROM clause - only relationship-based subqueries allowed
  SQL: SELECT * FROM (SELECT Name FROM Account) AS AccountNames;
  SOQL: Not supported
- Do not use fields that are not defined in the schema or data catalog. Always reference exact field names.

**FIELD SELECTION:**
- Always include Id field when querying
  CORRECT: SELECT Id, Name, Industry FROM Account
  INCORRECT: SELECT Name, Industry FROM Account
- Field names are case-sensitive
  CORRECT: SELECT CreatedDate FROM Account
  INCORRECT: SELECT createddate FROM Account
- Use exact field names from the data catalog
  CORRECT: SELECT CustomerPriority__c FROM Account
  INCORRECT: SELECT customer_priority FROM Account

**FILTERING (WHERE clause):**
- Date/DateTime fields: Use unquoted literals in YYYY-MM-DD or YYYY-MM-DDThh:mm:ssZ format
  CORRECT: WHERE CloseDate >= 2025-05-28
  CORRECT: WHERE CreatedDate >= 2025-05-28T10:30:00Z
  INCORRECT: WHERE CloseDate >= '2025-05-28'
  INCORRECT: WHERE CreatedDate >= "2025-05-28"
- Special date literals: TODAY, YESTERDAY, LAST_WEEK, LAST_MONTH, LAST_QUARTER, LAST_YEAR, THIS_WEEK, THIS_MONTH, THIS_QUARTER, THIS_YEAR
  CORRECT: WHERE CreatedDate = TODAY
  CORRECT: WHERE LastModifiedDate >= LAST_MONTH
  CORRECT: WHERE CloseDate >= THIS_QUARTER
- Date arithmetic (e.g., TODAY - 10) is not supported. Use literals like LAST_N_DAYS:10 instead.
  CORRECT: WHERE CloseDate >= LAST_N_DAYS:10
  INCORRECT: WHERE CloseDate >= TODAY - 10
- LIKE operator: Only supports % wildcard, NO underscore (_) wildcard
  CORRECT: WHERE Name LIKE '%Corp%'
  CORRECT: WHERE Name LIKE 'Acme%'
  INCORRECT: WHERE Name LIKE 'A_me%'
- BETWEEN operator: NOT supported, use >= AND <= instead
  SQL: WHERE CreatedDate BETWEEN '2025-01-01' AND '2025-12-31'
  SOQL: WHERE CreatedDate >= 2025-01-01 AND CreatedDate <= 2025-12-31
- Boolean values: Use lowercase true/false, NOT TRUE/FALSE
  CORRECT: WHERE Active__c = true
  CORRECT: WHERE IsDeleted = false
  INCORRECT: WHERE Active__c = TRUE
  INCORRECT: WHERE IsDeleted = FALSE
- NULL values: Use lowercase null, NOT NULL
  CORRECT: WHERE ParentId = null
  CORRECT: WHERE Description != null
  INCORRECT: WHERE ParentId IS NULL
  INCORRECT: WHERE Description IS NOT NULL
- String values: Use single quotes for strings
  CORRECT: WHERE Industry = 'Technology'
  CORRECT: WHERE Name = 'Acme Corp'
  INCORRECT: WHERE Industry = "Technology"
- Multi-select picklist fields: Use INCLUDES('value1;value2') or EXCLUDES('value1;value2')
  CORRECT: WHERE Services__c INCLUDES ('Consulting;Support')
  CORRECT: WHERE Services__c EXCLUDES ('Training')
  INCORRECT: WHERE Services__c = 'Consulting'
- Limited subquery support - only IN/NOT IN with non-correlated subqueries in WHERE clause
  CORRECT: SELECT Id FROM Contact WHERE Id NOT IN (SELECT WhoId FROM Task)
  INCORRECT: SELECT Id FROM Contact WHERE NOT EXISTS (SELECT 1 FROM Task WHERE WhoId = Contact.Id)

**JOINS:**
- NO explicit JOIN syntax supported
  SQL: SELECT a.Name, c.FirstName FROM Account a JOIN Contact c ON a.Id = c.AccountId
  SOQL: Not supported - use relationship traversal (not applicable in this use case)

**AGGREGATES:**
- NO COUNT(*) - use COUNT(Id) instead
  SQL: SELECT COUNT(*) FROM Account
  SOQL: SELECT COUNT(Id) FROM Account
- Cannot mix aggregate functions with non-aggregate fields unless using GROUP BY
  CORRECT: SELECT Industry, COUNT(Id) FROM Account GROUP BY Industry
  CORRECT: SELECT COUNT(Id) FROM Account
  INCORRECT: SELECT Industry, Name, COUNT(Id) FROM Account
- NO GROUP_CONCAT or string aggregation functions
  SQL: SELECT GROUP_CONCAT(Name) FROM Account
  SOQL: Not supported
- NO HAVING clause
  SQL: SELECT Industry, COUNT(*) FROM Account GROUP BY Industry HAVING COUNT(*) > 5
  SOQL: Not supported - filter with separate logic
- GROUP BY has limited field type support
  CORRECT: SELECT Industry, COUNT(Id) FROM Account GROUP BY Industry
  INCORRECT: SELECT Description, COUNT(Id) FROM Account GROUP BY Description (textarea fields not supported)

**FUNCTIONS:**
- Date functions: CALENDAR_MONTH(), CALENDAR_YEAR(), CALENDAR_QUARTER(), DAY_IN_MONTH(), DAY_IN_WEEK(), DAY_IN_YEAR(), HOUR_IN_DAY(), WEEK_IN_MONTH(), WEEK_IN_YEAR()
  CORRECT: SELECT Id, Name FROM Account WHERE CALENDAR_YEAR(CreatedDate) = 2025
  CORRECT: SELECT Id, Name FROM Account WHERE CALENDAR_MONTH(CreatedDate) = 5
  CORRECT: SELECT Id, Name FROM Account WHERE DAY_IN_WEEK(CreatedDate) = 2
- NO math functions: ROUND, FLOOR, CEILING, ABS, etc.
  SQL: SELECT ROUND(AnnualRevenue, 2) FROM Account
  SOQL: Not supported
- NO conditional functions: CASE WHEN, COALESCE, NULLIF, etc.
  SQL: SELECT CASE WHEN Industry = 'Technology' THEN 'Tech' ELSE 'Other' END FROM Account
  SOQL: Not supported
- NO string functions except INCLUDES/EXCLUDES for multi-select picklists
  SQL: SELECT UPPER(Name) FROM Account
  SOQL: Not supported

**OPERATORS:**
- Supported: =, !=, <, >, <=, >=, LIKE, IN, NOT IN, INCLUDES, EXCLUDES
  CORRECT: WHERE Industry = 'Technology'
  CORRECT: WHERE AnnualRevenue >= 1000000
  CORRECT: WHERE Industry IN ('Technology', 'Finance')
  CORRECT: WHERE Industry NOT IN ('Government', 'Non-Profit')
  CORRECT: WHERE Services__c INCLUDES ('Consulting')
- NOT supported: REGEXP, BETWEEN, EXISTS, NOT EXISTS
  SQL: WHERE Name REGEXP '^[A-Z]'
  SOQL: Not supported

**SORTING & LIMITING:**
- ORDER BY: Fully supported
  CORRECT: SELECT Id, Name FROM Account ORDER BY Name ASC
  CORRECT: SELECT Id, Name FROM Account ORDER BY CreatedDate DESC, Name ASC
  CORRECT: SELECT Id, Name FROM Account ORDER BY Name NULLS LAST
- LIMIT: Maximum 2000 records, use smaller limits for better performance
  CORRECT: SELECT Id, Name FROM Account LIMIT 100
  CORRECT: SELECT Id, Name FROM Account LIMIT 2000
  INCORRECT: SELECT Id, Name FROM Account LIMIT 5000
- NO OFFSET: Not supported for pagination
  SQL: SELECT Id, Name FROM Account LIMIT 10 OFFSET 20
  SOQL: Not supported

**DATA TYPES:**
- picklist: Single-select dropdown, use = operator with string values
  CORRECT: WHERE Industry = 'Technology'
  CORRECT: WHERE Rating = 'Hot'
- reference: Foreign key field, typically ends with Id
  CORRECT: WHERE OwnerId = '00530000003OOwn'
  CORRECT: WHERE AccountId = '0013000000UzXyz'
- boolean: Use lowercase true/false
  CORRECT: WHERE IsDeleted = false
  CORRECT: WHERE Active__c = true
- currency: Numeric field for money values
  CORRECT: WHERE AnnualRevenue > 1000000
  CORRECT: WHERE AnnualRevenue >= 500000.50
- date: Date only, use YYYY-MM-DD format
  CORRECT: WHERE LastActivityDate = 2025-05-28
  CORRECT: WHERE SLAExpirationDate__c >= 2025-01-01
- datetime: Date and time, use YYYY-MM-DDThh:mm:ssZ format
  CORRECT: WHERE CreatedDate >= 2025-05-28T10:30:00Z
  CORRECT: WHERE LastModifiedDate = 2025-05-28T00:00:00Z
- double/int: Numeric fields
  CORRECT: WHERE NumberOfEmployees > 100
  CORRECT: WHERE NumberofLocations__c >= 5.5
- string/textarea: Text fields, use single quotes
  CORRECT: WHERE Name = 'Acme Corporation'
  CORRECT: WHERE Description = 'Leading tech company'
- phone/url/email: Specialized string fields, treat as strings
  CORRECT: WHERE Phone = '555-1234'
  CORRECT: WHERE Website = 'https://example.com'

**COMMON MISTAKES TO AVOID:**
- Using SELECT * (not allowed)
  WRONG: SELECT * FROM Account
  RIGHT: SELECT Id, Name, Industry FROM Account
- Quoting date literals (dates must be unquoted)
  WRONG: WHERE CreatedDate >= '2025-01-01'
  RIGHT: WHERE CreatedDate >= 2025-01-01
- Using SQL JOIN syntax (not supported)
  WRONG: SELECT Account.Name FROM Account JOIN Contact ON Account.Id = Contact.AccountId
  RIGHT: Use relationship traversal (not applicable in this use case)
- Using BETWEEN operator (not supported)
  WRONG: WHERE CreatedDate BETWEEN 2025-01-01 AND 2025-12-31
  RIGHT: WHERE CreatedDate >= 2025-01-01 AND CreatedDate <= 2025-12-31
- Using uppercase TRUE/FALSE/NULL (must be lowercase)
  WRONG: WHERE Active__c = TRUE
  RIGHT: WHERE Active__c = true
- Using underscore _ in LIKE patterns (only % supported)
  WRONG: WHERE Name LIKE 'A_me%'
  RIGHT: WHERE Name LIKE 'A%me%'
- Mixing aggregate and non-aggregate fields without GROUP BY
  WRONG: SELECT Name, COUNT(Id) FROM Account
  RIGHT: SELECT Industry, COUNT(Id) FROM Account GROUP BY Industry

**EXAMPLE QUERIES:**
- Basic selection: SELECT Id, Name, Industry FROM Account WHERE Industry = 'Technology'
- Date filtering: SELECT Id, Name FROM Account WHERE CreatedDate >= 2025-01-01
- Multiple conditions: SELECT Id, Name FROM Account WHERE Name LIKE '%Corp%' AND Industry IN ('Technology', 'Finance')
- Aggregation: SELECT Industry, COUNT(Id) FROM Account GROUP BY Industry
- Boolean and numeric: SELECT Id, Name FROM Account WHERE Active__c = true AND NumberOfEmployees > 100
- Date functions: SELECT Id, Name FROM Account WHERE CALENDAR_YEAR(CreatedDate) = 2025
- Null checks: SELECT Id, Name FROM Account WHERE ParentId = null
- Multi-select picklist: SELECT Id, Name FROM Account WHERE Services__c INCLUDES ('Consulting;Support')
- Sorting and limiting: SELECT Id, Name FROM Account ORDER BY Name ASC LIMIT 50


***EXECUTION INSTRUCTIONS. IMPORTANT!***
After generating the core SOQL (and nothing else), always make sure you wrap it exactly as:

    SELECT * 
      FROM {integration_name}(
        /* your generated SOQL goes here, without a trailing semicolon */
      )

Return only that wrapper call.
"""
