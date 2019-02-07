import mindsdb


mdb = mindsdb.MindsDB(send_logs=False)

mdb.learn(
    from_data='ct.csv',
    predict='Retweets',
    model_name='run_a_file'
)
print('!-------------  Learning ran successfully  -------------!')

''':
features = {}
result = mdb.predict(when=features, model_name='run_a_file')
print(result)
'''
print('!-------------  Prediction from file ran successfully  -------------!')
