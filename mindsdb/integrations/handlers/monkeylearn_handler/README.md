### Briefly describe what ML framework does this handler integrate to Mindsdb, and how ? 

This handler integrates Mindsdb to Monkeylearn. Monkeylearn is a tool which comes with prebuilt & custom Machine learning models code free. It simplifies text analytics with templates, like Keyword + Classification template and Business templates. MonkeyLearn integrates with Mindsdb with the help of monkeylearn's **REST API** .It can be used to classify text according to user needs and field of interest like business, reviews, comments and customer-feedback.


### Why is this integration useful? What does the ideal predictive use case for this integration look like? When would you definitely not use this integration?

This integration can really be useful for custom models which are trained by user on the Monkeylearn website which has good UI/UX, allowing users to train model code free and use it furthur with **model_id**.

This integration shouldn't be considered when using the free **Monkeylearn** API key which has limited number of queries per account. This problem can be tackled by having a premium plan of MonkeyLearn which has custom queries according to user needs. 

The monkeylearn classifier API only accepts list of upto 500 data elements of string with text to classify in the data attribute. [Classifier doc](https://monkeylearn.com/api/v3/#classify)

### To what degree can users control the underlying framework by passing parameters via the USING syntax?

Users can change the model_id parameter according to the model of their need or custom model built by them.

### Does this integration offer model explainability or insights via the DESCRIBE syntax?

Yes this integrations supports insights via the **DESCRIBE** 

### Any directions for future work in subsequent versions of the handler?

This integrations currently works with only classifier models on monkeylearn. Other models like extractor and workflow can be integrated in furthur versions of this handler.


### Please provide a minimal SQL example that uses this ML engine (pointers to integration tests in the PR also valid)
[Loom Video](https://www.loom.com/share/0b9c477a9c2845aa94c72d56b621203d) this loom video demonstrates SQL example to use the integration