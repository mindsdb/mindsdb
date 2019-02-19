import mindsdb


MindsDB().learn(
    from_data="train.csv",
    predict='Survived',
    model_name='titanic_model'
)
