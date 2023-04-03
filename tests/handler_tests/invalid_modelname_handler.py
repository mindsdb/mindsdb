import pytest
import mindsdb
import openai

def test_invalid_openai_name_parameter():
    openai.api_key = 'YOUR_API_KEY' # Replace with your OpenAI API key
    available_models = [model.id for model in openai.Model.list()]
    with pytest.raises(ValueError, match=r'.*Invalid model name.*'):
        mindsdb.Predictor(name='invalid_model_name', model_type='nlp', predict_when='accuracy', query={
            'model_name': 'non_existing_model',
            'engine': 'openai',
            'json_struct': {
                'rental_price': 'rental price',
                'location': 'location',
                'nob': 'number of bathrooms'
            },
            'input_text': 'sentence'
        })
