
### Briefly describe what ML framework does this handler integrate to MindsDB, and how? 
This handler integrates [Replicate](https://replicate.com/), which is a place to share and run your machine learning models. Replicate lets you run machine learning models with a cloud API, without having to understand the intricacies of machine learning or manage your own infrastructure. You can run open-source models that other people have published, or package and publish your own models. Those models can be public or private.


### Why is this integration useful? What does the ideal predictive use case for this integration look like? When would you definitely not use this integration? 
Replicate have many kind of model like img2img, text2img, img2text, LLM, Audio generation and many more. Most of all this model can directly be used from MindsDB. You can find model matching your usecase from replicate and the use it from MindsDB.

### Are models created with this integration fast and scalable, in general?
Of Course, model are scalable but Replicate offers a free tier that allows users to generate a limited number of predictions. However, by subscribing to their service, you gain access to additional resources such as CPU or GPU, enabling faster and more efficient predictions, ultimately reducing prediction time.

### What are the recommended system specifications for models created with this framework?
N/A

### To what degree can users control the underlying framework by passing parameters via the USING syntax?
Every model have different parameters which can be used for customizing prediction through **USING** syntax. You can get paramaeter list by executing `DESCRIBE PREDICTOR mindsdb.<model_name>.features`.

### Does this integration offer model explainability or insights via the DESCRIBE syntax?
`DESCRIBE PREDICTOR mindsdb.<model_name>.features` can be used to see list of parameters can be use with model.

### Does this integration support fine-tuning pre-existing models (i.e. is the update() method implemented)? Are there any caveats?
No

### Are there any other noteworthy aspects to this handler?
Most remarkable aspects of this handler is its wide range of model categories, including audio generation, diffusion models, text-to-image, style transfer, language model, image-to-text, videos, and many more. These models can be seamlessly integrated into MindsDB, enhancing its capabilities and providing diverse functionalities for various tasks

### Any directions for future work in subsequent versions of the handler?
Yes, Of Course few that comes to my mind are 
- [ ] prompt template
- [ ] NLP to parameter generation 
- [ ] Storing Predicted images, videos and audio to some persistent storage because replicate URL is only available for 24 hours


### Please provide a minimal SQL example that uses this ML engine (pointers to integration tests in the PR also valid)
[Example](./Manual_QA.md)

## Usage
To use this handler and connect to a Replicate cluster in MindsDB, you need an account on Replicate. Make sure to create an account by following this [link](https://replicate.com/signin?next=/account/api-tokens).

- [Audio](./usage_audio.md)
    - [Audio Generation](./usage_audio.md#audio-generation)
    - [Custom Audio Cloning](./usage_audio.md#custom-audio-cloning)
- [img2text](./usage_img2text.md)
    - [Image Captioning](./usage_img2text.md#image-captioning)
    - [Image Text Matching](./usage_img2text.md#visual-question-answering)
- [LLM](./usage_LLM.md)
- [text2img](./usage_text2img.md)
- [text2vid](./usage_text2video.md)