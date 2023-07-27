# Replicate Handler

This is the implementation of the Replicate handler for MindsDB.

## Replicate
Replicate is a platform and tool that aims to make it easier to work with machine learning models. It provides a library of open-source machine learning models that can be run in the cloud. Replicate allows users to deploy their own machine learning models at scale by providing infrastructure and automatic generation of API servers.


## Implementation
This handler was implemented using the `replicate` library that is provided by Amazon Web Services.

The required arguments to establish a connection are,
- 
* model_name: Model name which you want to access in MindsDB. e.g 'air-forever/kandinsky-2'
* version: version hash/id which you want to use in MindsDB.
* api_key: API key from Replicate Platform you can found [here](https://replicate.com/account/api-tokens).


## Usage
To use this handler and connect to a Replicate cluster in MindsDB, you need an account on Replicate. Make sure to create an account by following this [link](https://replicate.com/signin?next=/account/api-tokens).

- [audio](./usage_audio.md)
    - [Audio Generation](./usage_audio.md#audio-generation)
    - [Custom Audio Cloning](./usage_audio.md#custom-audio-cloning)
- [img2text](./usage_img2text.md)
    - [Image Captioning](./usage_img2text.md#image-captioning)
    - [Image Text Matching](./usage_img2text.md#visual-question-answering)
- [LLM](./usage_LLM.md)
- [text2img](./usage_text2img.md)
- [text2vid](./usage_text2video.md)
