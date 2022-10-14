<h1 align="center">
	<img width="1500" src="/assets/hacktoberfest.png" alt="MindsDB">
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

##  ~~ML Frameworks Ideas Challenge~~ (Finished - October 1st) 📖 📝 🚧

<details>
  <summary> Check out the details and winners (Click to expand!)</summary>
	
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

📢 🎉
* 1st Prize 🥇: [Arman Chand, who suggested integration with Tensorflow](https://github.com/mindsdb/mindsdb/issues/2933) with 67 👍
* 2nd Prize 🥈: [Rutam Prita Mishra, who suggested integration with Apache SINGA](https://github.com/mindsdb/mindsdb/issues/3040) with 50 👍
* 3rd Prize 🥉: [Noel Jacob, who suggested integration with Fast.ai](https://github.com/mindsdb/mindsdb/issues/2941) with 40 👍
</details>

# Hacktoberfest ( October 1st - 31st) :octocat: :female_detective: :male_detective:

We have prepared several categories of open issues that are ideal for first-time developers, low-code, and no-code contributors.  
They fall into the following categories:

* [Code, Low & No-Code](https://github.com/mindsdb/mindsdb#code-low--no-code)
* [Video Tutorials](https://github.com/mindsdb/mindsdb#video-tutorials)
* [Integration Ideas](https://github.com/mindsdb/mindsdb#integration-ideas)


## Code, Low & No-Code 👨‍💻 👩‍💻

Contributing to open-source isn;t just for technical folks who want to write code. There are lots of opportunities to use your professional skills in support of open-source projects.

MindsDB engineers have prepared several categories of [open issues](https://github.com/mindsdb/mindsdb/labels/hacktoberfest) that are ideal for first-time contributors. It’s entirely up to you what you choose to work on and if you have your own great idea, feel free to suggest it by reaching out to us via our [Slack community](https://mindsdb.com/joincommunity) or [GitHub Discussions](https://github.com/mindsdb/mindsdb/discussions).

  
#### Prizes 🎁

1.  The main prize draw is the awesome [Razer Blade 15 Laptop](https://www.razer.com/hard-bundles/Razer-Blade-15-Bundle-V5-Essential/RZHB-220912-11) **powered by the top** **NVIDIA GeForce RTX 3080 Ti GPU, 14-core Intel i9 CPU and more…**
2.  Additional prize draw – 10 audio gadgets of your choice ([JBL Earbuds](http://mindsdb.com/wp-content/uploads/2022/09/earbuds.jpg) or [Rugged Outdoor Speaker](http://mindsdb.com/wp-content/uploads/2022/09/Speaker.jpg)). Each winner can choose one of the gadgets.
3.  Anyone who contributes with at least 5 points, will receive a MindsDB t-shirt.
‍

### See the Rules

<details>
  <summary>(Click to expand!)👈 </summary>
	
##### How to participate? 🏁

1.  Check our [contributing guidelines](https://docs.mindsdb.com/contribute/install/).
2.  For code & low code contributions – make a pull request to any of our open [issues labeled Hacktoberfest](https://github.com/mindsdb/mindsdb/labels/hacktoberfest) during the October timeframe and ensure it is merged.
3.  If you create a new tutorial (or video tutorial) you will need to make a PR to our documentation, adding a link of it to our community-supported tutorials.
4.  ‍[Complete the form](https://forms.gle/WDWuhiAwg1tSmBF6A) with your details and links to all your merged PR’s or no-code deliverables  

**Contributions and Points** ⭐

-   [1 point] Docs contribution, Report a Bug, QA/UX (Manual Test), Develop an Architectural Diagram
-   [1 point] Social Media Posting (min 2 posts, points will not sum up)
-   [3 points] Bug Fix or QA/UX (Automated Test)
-   [5 points] Write a Tutorial, Case Study, or Submit a UX Design
-   [7 points] Data-Source Integration
-   [9 points] Video Tutorial
-   [13 points] ML Framework Integration  
  
##### Rules ⚙️

1.  For every 10 points of contributions, you will receive one entry into the prize draw. You need a minimum of 10 points to participate in the draw; the more entries you get, the higher your chances of winning are.
2.  You can submit as many deliverables of the same type as you wish, to earn more points (except for social posting).
3.  Please make good-quality contributions. We may reject formal, nearly duplicative, low-quality submissions. No manipulations for earning points!
4.  To participate, you need to [complete this form](https://forms.gle/WDWuhiAwg1tSmBF6A) with your contact details and links to all your merged PRs or no-code deliverables. Submit the form once with all your work. You can edit responses after submission.
5.  Entries will close at midnight (PST) Monday, 31st of October.

📣 We will make the draw and announce the winners on our [Community Slack](https://mindsdb.com/joincommunity) during the first week of November.
📣 If shipping the prizes to your country requires complex customs procedures, we might provide you with their monetary value.

</details>

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

## Video Tutorials 🎥 ⏯️

Make as many contributions of your choice to win a high-end Razer Blade 15 Laptop or one of 10 cool audio gadgets!

Make video tutorials of using MindsDB and invite everyone to watch them on YouTube. The authors of the most popular videos will get a T-shirt, cool audio gadgets, and a cash prize:

#### Prizes 🎁  

1.  The first place gets **$1000** cash prize, plus MindsDB t-shirt, plus the gadget of choice
2.  The second place gets **$500** cash prize, plus MindsDB t-shirt, plus the gadget of choice
3.  The third place gets **$300** cash prize, plus MindsDB t-shirt, plus the gadget of choice

### See the Rules

<details>
  <summary>(Click to expand!)👈</summary>

##### How to participate? 🏁

1.  Record a video tutorial and make it public on YouTube.
2.  Make a GitHub Pull Request to our documentation, with adding a link of it to our community-supported video tutorials.
3.  When PR is merged, invite everyone to watch it.
4.  Share it on the [MindsDB Slack community](https://mindsdb.com/joincommunity) at the [#using_mindsdb](https://join.slack.com/share/enQtMzU5ODc5OTMzMDYzMC1iYzc1MmFkMjY0MDQ0MmM0OTM2ZWY0MzU2NWY2NjBmM2I5MjZlN2JlZDIzN2M4MzQwNzY3MzJhMjJmNjcyYWM1) channel
5.  [Complete the form](https://forms.gle/WDWuhiAwg1tSmBF6A) with your details and links to all your video tutorials and other contributions  
    ‍  
##### Rules ⚙️

1.  We will only count videos [submitted through the form](https://forms.gle/WDWuhiAwg1tSmBF6A) for the competition.
2.  We will use the point system to calculate the popularity of the video based on their YouTube public stats. **Every watch counts as 1 point. Every like counts as 5 points.**
3.  We will summarize the points for each video on the **14th of November at 9:00 AM PST** so that you can have more time for promotion.
4.  The top 3 authors, whose video tutorials will get the highest number of points, are the winners. If multiple videos have the same number of points, the first one posted wins. Please feel free to submit as many videos, as you like. However, we will not sum up points from multiple videos from a single author. We’ll take just one that has the highest number of points.
📣 Rewards will be paid as one-time [GitHub sponsorship](https://docs.github.com/en/sponsors/receiving-sponsorships-through-github-sponsors). Each winner can choose one of the gadgets ([JBL Earbuds](http://mindsdb.com/wp-content/uploads/2022/09/earbuds.jpg) or [Rugged Outdoor Speaker](http://mindsdb.com/wp-content/uploads/2022/09/Speaker.jpg))[  
](https://forms.gle/WDWuhiAwg1tSmBF6A)
	
</details>

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

## Integration Ideas 💡 🔁

We believe that offering a simple way to implement Machine Learning where the data lives, can bring improvements to the design of the AI applications and their adoption. That’s why we want to hear your ideas and bring different ML Frameworks to different data sources. Currently, MindsDB supports 40+ different data sources and few ML Frameworks.

#### Prizes 🎁

1.  The first place gets **$500** cash prize, plus MindsDB t-shirt, plus audio gadget
2.  The second place gets **$300** cash prize, plus MindsDB t-shirt, plus audio gadget
3.  The third place gets **$200** cash prize, plus MindsDB t-shirt, plus audio gadget

### See the Rules

<details>
  <summary>(Click to expand!)👈</summary>
  
##### How to participate? 🏁

1.  Create a new [GitHub issue using this template](https://github.com/mindsdb/mindsdb/issues/new?assignees=&labels=integration%2Cenhancement&template=integrations_contest.yaml&title=%5BNew+Integration%5D%3A+).
2.  Invite everyone to upvote by adding 👍 thumbs up emoji to an issue.
3.  Share it on the [MindsDB Slack community](https://mindsdb.com/joincommunity) at the [#using_mindsdb](https://join.slack.com/share/enQtMzU5ODc5OTMzMDYzMC1iYzc1MmFkMjY0MDQ0MmM0OTM2ZWY0MzU2NWY2NjBmM2I5MjZlN2JlZDIzN2M4MzQwNzY3MzJhMjJmNjcyYWM1) channel
‍
##### Rules ⚙️

1.  MindsDB team will review the ideas and label them as _accepted_.
2.  People will start voting 👍 on the proposed ideas and discuss further implementation.
3.  The top 3 authors, whose ideas will get the highest number of upvotes, are the winners. If multiple ideas have the same number of votes, the first one posted wins.
4.  You can submit as many ideas as you like. However, we will not sum up upvotes from multiple issues from a single author. We’ll take just one that has the highest number of votes.

📣 Rewards will be paid as one-time [GitHub sponsorship](https://docs.github.com/en/sponsors/receiving-sponsorships-through-github-sponsors). Each winner can choose one of the gadgets ([JBL Earbuds](http://mindsdb.com/wp-content/uploads/2022/09/earbuds.jpg) or [Rugged Outdoor Speaker](http://mindsdb.com/wp-content/uploads/2022/09/Speaker.jpg))[  
](https://github.com/mindsdb/mindsdb/issues/new?assignees=&labels=integration%2Cenhancement&template=integrations_contest.yaml&title=%5BNew+Integration%5D%3A+)
	
</details>

#### Contact us📨
If you have any questions/ideas or need to discuss your work – please reach out via our [Slack](https://mindsdb.com/joincommunity)‍[  
](https://github.com/mindsdb/mindsdb/issues/new?assignees=&labels=integration%2Cenhancement&template=integrations_contest.yaml&title=%5BNew+Integration%5D%3A+)

📣 We will announce the winners on our [Community Slack](https://mindsdb.com/joincommunity) during the first couple of days after the submission deadline.

### What’s coming soon? ⌛

In the coming weeks, we will add more challenges like writing tutorials, new integrations, and many more, so stay tuned. And if you like MindsDB, we would appreciate you sharing your love by awarding us a GitHub Star ⭐
