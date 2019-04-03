from mindsdb import Predictor


mdb = Predictor(name='marvel')

mdb.learn(from_data="marvel-wikia.xlsx", to_predict='FIRST_APPEARANCE')

print('------------------------------------------------------------Done training------------------------------------------------------------')
"""
predicted = mdb.predict(when={
    'Date':'11/03/2020',
    'Time':'18.00.00',
    'NMHC_GT': 1360.0,
    'AH': 0.655
})
print('------------------------------------------------------------Preidiction output------------------------------------------------------------')
for val in predicted:
    print(val['CO_GT'])
    print(val['CO_GT_confidence'])
"""
