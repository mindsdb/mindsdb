<h1 align="center">
	<img width="1500" src="/assets/dev-contest.png" alt="MindsDB">
	<br>
</h1>

# Democratize Machine Learning on ... Contest 🎆 🎉

MindsDB is an open-source project on a quest to democratize machine learning and take it closer to your data, and we want you to join in. 

The *Democratize Machine Learning on ... Contest* is a competition for data professionals, developers, machine learning experts, technical managers, students, business analysts, decision-makers, etc.

We invite you to contribute to our project at the open brainstorming session by telling us where you would connect MindsDB to generate predictions. Currently, we support most of the SQL databases out there.

## ~~Ideas Challenge~~ 💡 (Finished 17th June)

<details>
  <summary> Check out the details and winners (Click to expand!)</summary>

Propose an idea of integrations that MindsDB should support. These could be database platforms, ML frameworks, API integrations, etc. We leave it to your imagination!

#### Rewards 🏅

The `TOP-3` authors who submit ideas before `June 17th, 2022` get SWAG and other cool prizes.

| Place | Prize|
--------|--------
| 1st| $1000|
| 2nd| $500|
| 3th| $250|

> Rewards will be paid as one-time GitHub sponsorships.

### How to participate? 🏁

1. Create a new [GitHub issue](https://github.com/mindsdb/mindsdb/issues/new?assignees=&labels=integration%2Cenhancement&template=integrations_contest.yaml&title=%5BNew+Integration%5D%3A+).
2. Invite everyone to upvote by adding 👍(thumbs up) emoji to your issue.
3. Share it on the MindsDB Slack community at [the `using_mindsdb` channel](https://join.slack.com/share/enQtMzU5ODc5OTMzMDYzMC1iYzc1MmFkMjY0MDQ0MmM0OTM2ZWY0MzU2NWY2NjBmM2I5MjZlN2JlZDIzN2M4MzQwNzY3MzJhMjJmNjcyYWM1).
4. [Fork MindsDB repository](https://github.com/mindsdb/mindsdb/fork) (optional).
5. Commit the basic structure for the new integration (optional):
	 * [ ] Add a new integration directory under `integrations`.
	 * [ ] Add `__about__.py` file containing all variables as this example.
	 * [ ] Add empty `tests` directory.
	 * [ ] Add empty `__init__.py` file.
6. Make a Pull Request to MindsDB Repository from your fork and tag the idea issue.

### Rules 🚥

* MindsDB team will review the ideas and label them as `accepted`. Once an idea is reviewed and accepted, it is moved to the [`Integrations ideas` project](https://github.com/mindsdb/mindsdb/projects/9). 
* People will start voting on the proposed ideas and discuss further implementation.
* The top 3 authors, whose ideas will get the highest number of upvotes 👍, are the winners. If multiple ideas have the same number of votes, the first one posted wins. Please feel free to submit as many ideas as you like. However, we will not sum up upvotes from multiple issues from a single author. We'll take just one that has the highest number of votes. 
* Make sure you complete all the steps above to be considered for a prize. After the deadline, we move the issues with the ideas to the `Implementation ideas` phase, where the number of votes they scored is added as a comment. Note that we only count the votes from the accounts created at least a month before the contest started to avoid vote-rigging.

📣 ~~We will announce the winners on our [Community Slack](https://mindsdb.com/joincommunity) during the first couple of days after the submission deadline.~~

📢 🎉
* 1st Prize 🥇: $1,000 for [Supabase integration](https://github.com/mindsdb/mindsdb/issues/2315) opened by Ditmar Chetelev with 60 :+1:
* 2nd Prize 🥈: $500 for [Integration for open-source ORM Prisma](https://github.com/mindsdb/mindsdb/issues/2361) opened by Arman Chand with 57 :+1:
* 3rd Prize 🥉: $250 for [Integration as a Marketplace App for leading Cloud Providers](https://github.com/mindsdb/mindsdb/issues/2342) opened by Rutam Prita Mishra with 55 :+1:
	
</details>


## ~~Dev challenge~~ (Finished - September 1st) 👩‍💻 👨‍💻 

<details>
  <summary> Check out the details and winners (Click to expand!)</summary>
  
In the `Ideas challenge`, MindsDB community members have shared 53 ideas. In this challenge, we will implement them. To participate, check out the [ideas dashboard](https://github.com/mindsdb/mindsdb/projects/9) and follow the rules. If you want to work on an integration that is not included in the list, feel free to [open a new issue](https://github.com/mindsdb/mindsdb/issues/new?assignees=&labels=integration%2Cenhancement&template=integrations_contest.yaml&title=%5BNew+Integration%5D%3A+) that we'll assign to you.

#### Rewards 🏅

For every integration created, you win `USD 200` and [SWAG](https://mindsdb.com/community/). If you create more than 3 integrations, you get an additional `USD 200`. We will pay out the rewards as a [GitHub Sponsorships](https://github.com/sponsors) or a bank transfer if GitHub Sponsorship is not available in your country.

### How to participate? 🏁

1. Comment on the integration you want to start implementing, so the MindsDB team can assign that issue to you.
> If you don't commit anything within five days, the issue may be assigned to someone else.
2. [Fork MindsDB repository](https://github.com/mindsdb/mindsdb/fork) and start coding.
3. Check the [Build new integration docs](https://docs.mindsdb.com/contribute/integrations/).
3. Join our [Slack community](https://mindsdb.com/joincommunity) to discuss and ask questions.

### Rules 🚥

Here are the requirements for an implementation to be accepted.
* Fully working integration.
* Tests to demonstrate that integration works.
* Documentation (`README` file)
    * Description
    * Required configuration
    * How to run tests
    
> You can only work on one integration at a time. Once you submit the PR, you can start working on another integration. To win a prize, the PR must be submitted and merged before September 1st, 2022.

</details>

## ML Frameworks Ideas Challenge 📖 📝 🚧

We believe that offering a simple way to implement Machine Learning where the data lives, can bring improvements to the design of the ML applications and their adoption. That's why we want to hear your ideas and bring different ML Frameworks to different data sources. Currently, MindsDB supports +40 different data sources and few ML Frameworks. Some examples of the current ideas we have and the supported ML integrations:

* [MLFlow](https://github.com/mindsdb/mindsdb/tree/staging/mindsdb/integrations/handlers/mlflow_handler)
* [Lightwood](https://github.com/mindsdb/mindsdb/tree/staging/mindsdb/integrations/handlers/lightwood_handler)
* [Ludwig](https://github.com/mindsdb/mindsdb/tree/staging/mindsdb/integrations/handlers/ludwig_handler)
* [🚧 Huggingface]() 
* [🚧 Ray Serve ]()

This challenge is similar to the Phase 1 Ideas challenge, but here we consider only the ML Frameworks ideas. The `TOP-3` authors who submit ideas before `October 1st, 2022` get SWAG and other cool prizes:

| Place | Prize|
--------|--------
| 1st| $1000|
| 2nd| $500|
| 3th| $250|

> Rewards will be paid as one-time GitHub sponsorship.

### How to participate? 🏁

1. Create a new [GitHub issue](https://github.com/mindsdb/mindsdb/issues/new?assignees=&labels=integration%2Cenhancement&template=integrations_contest.yaml&title=%5BNew+Integration%5D%3A+).
2. Invite everyone to upvote by adding 👍(thumbs up) emoji to an issue.
3. Share it on the MindsDB Slack community at [the `using_mindsdb` channel](https://join.slack.com/share/enQtMzU5ODc5OTMzMDYzMC1iYzc1MmFkMjY0MDQ0MmM0OTM2ZWY0MzU2NWY2NjBmM2I5MjZlN2JlZDIzN2M4MzQwNzY3MzJhMjJmNjcyYWM1)

### Rules 🚥

* MindsDB team will review the ideas and label them as `accepted`. Once the idea is reviewed and accepted, it is moved to the [`ML Frameworks Integrations ideas` project](https://github.com/mindsdb/mindsdb/projects/10). 
* People will start voting on the proposed ideas and discuss further implementation. 
* The top 3 authors, whose ideas will get the highest number of upvotes 👍, are the winners. If multiple ideas have the same number of votes, the first one posted wins. Please feel free to submit as many ideas as you like. However, we will not sum up upvotes from multiple issues from a single author. We'll take just one that has the highest number of votes. 
* Make sure you complete all the steps above to be considered for a prize. After the deadline, We move the issues with the ideas to the `Implementation` phase, where the number of votes they scored is added as a comment. Note that we only count the votes from the accounts created at least a month before the contest started to avoid vote-rigging.

📣 We will announce the winners on our [Community Slack](https://mindsdb.com/joincommunity) during the first couple of days after the submission deadline.

### What’s coming soon? ⌛

In the coming weeks, we will add more challenges like writing tutorials, new integrations, and many more, so stay tuned. And if you like MindsDB, we would appreciate you sharing your love by awarding us a GitHub Star ⭐
