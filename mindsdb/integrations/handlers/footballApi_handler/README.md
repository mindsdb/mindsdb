# FootBall API Handler  
  
This handler integrates with the [API Football](https://www.api-football.com/) to retrieve player information.  
  
  
## Connect to the Football API  
We start by creating a database to connect to the Football API. You'll need an access token which can be accessed from [RAPIDAPI](https://api-football-v1.p.rapidapi.com/v3/) or  [API-SPORTS](https://v3.football.api-sports.io/) and the domain you want to send API requests to.  
  
Example  
```  
CREATE  DATABASE my_mindsdb_footballapi

WITH
ENGINE  =  'footballapi'
PARAMETERS  = {
"api_key": "TOKEN HERE",
"account_type": "TYPE HERE" // "api-sports" or "rapid-api"
};
```  
  
;## Get Player Info 
To get the player information, use the following query 
  
```  
SELECT player_id, player_name, player_firstname
FROM my_mindsdb_footballapi.get_players
WHERE league=39  AND season=2021 AND page=3;  
```  
-  id = id of the player
- team = id of the team
- league = id of the league
- season = season of the league
- page = how many page required

  

```