### Briefly describe what ML framework does this handler integrate to MindsDB, and how? 
This handler integrates TPOT, which is a machine learning automation framework, to MindsDB. TPOT uses genetic programming to automate the process of building and optimizing machine learning models, and the integration with MindsDB allows users to easily incorporate TPOT's automated model selection and hyperparameter tuning capabilities into their machine learning workflows.


### Why is this integration useful? What does the ideal predictive use case for this integration look like? When would you definitely not use this integration? 
An ideal predictive use case for this integration would be a situation where a user has a large amount of data and wants to build a predictive model without spending a significant amount of time on feature engineering, model selection, and hyperparameter tuning. By using TPOT with MindsDB, the user can automate these processes and quickly generate a high-quality predictive model.

### Are models created with this integration fast and scalable, in general?
In general, models created with this integration are fast and scalable due to the automation of model selection and hyperparameter tuning provided by TPOT. However, the performance and scalability of the final model will also depend on the size and complexity of the data, as well as the resources available for training and deployment.
### What are the recommended system specifications for models created with this framework?
N/A

### To what degree can users control the underlying framework by passing parameters via the USING syntax?
- **generations**(default=10 ): Optional, Number of iterations to the run pipeline optimization process.Generally, TPOT will work better when you give it more generations (and therefore time) to optimize the pipeline.

- **population_size**(default=100): Optional, Number of individuals to retain in the genetic programming population every generation. Must be a positive number. Generally, TPOT will work better when you give it more individuals with which to optimize the pipeline. 
- **max_time_mins**(default=None): Optional, How many minutes TPOT has to optimize the pipeline.
If not None, this setting will allow TPOT to run until max_time_mins minutes elapsed and then stop. TPOT will stop earlier if generations is set and all generations are already evaluated. 
- **n_jobs**(default=-1): Optional, Number of processes to use in parallel for evaluating pipelines during the TPOT optimization process. Setting n_jobs=-1 will use as many cores as available on the computer. For n_jobs below -1, (n_cpus + 1 + n_jobs) are used. Thus for n_jobs = -2, all CPUs but one are used. Beware that using multiple processes on the same machine may cause memory issues for large datasets. 

### Does this integration offer model explainability or insights via the DESCRIBE syntax?

Not Supported Now.

### Does this integration support fine-tuning pre-existing models (i.e. is the update() method implemented)? Are there any caveats?
No

### Are there any other noteworthy aspects to this handler?
Yes, another noteworthy aspect of the TPOT ML handler is that it allows for the automatic feature engineering of data. TPOT uses a combination of machine learning and genetic programming to explore the space of possible feature combinations, and can identify relevant features that a user may not have considered. This can lead to improved model accuracy and reduced time spent on manual feature engineering.

### Any directions for future work in subsequent versions of the handler?
Tpot is adding new feature with every release, therefore we can also implemented them in our Handler

### Please provide a minimal SQL example that uses this ML engine (pointers to integration tests in the PR also valid)
[Example](./Manual_QA.md)


