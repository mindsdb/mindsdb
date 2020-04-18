---
id: booking-cancelation
title: Hotel Booking Demand 
---

| Industry       | Department | Role               |
|----------------|------------|--------------------|
| Travel | Operations | Business executive |

## Processed Dataset 

###### [![Data](https://img.shields.io/badge/GET--DATA-HotelBooking-green)](https://github.com/mindsdb/mindsdb-examples/tree/master/benchmarks/hotel-booking/dataset)

This [data set](https://www.kaggle.com/jessemostipak/hotel-booking-demand) contains booking information for a city hotel and a resort hotel, and includes information such as when the booking was made, length of stay, the number of adults, children, and/or babies, and the number of available parking spaces, among other things.

<details>
  <summary>Click to expand Features Informations:</summary>
1. hotelHotel (H1 = Resort Hotel or H2 = City Hotel)
2. is_canceled Value indicating if the booking was canceled (1) or not (0)
lead_timeNumber of days that elapsed between the entering date of the booking into the PMS and the arrival date
3. arrival_date_year Year of arrival date
4. arrival_date_month Month of arrival date
5. arrival_date_week_number Week number of year for arrival date
6. arrival_date_day_of_month Day of arrival date
7. stays_in_weekend_nights Number of weekend nights (Saturday or Sunday) the guest stayed or booked to stay at the hotel
8. stays_in_week_nightsNumber of week nights (Monday to Friday) the guest stayed or booked to stay at the hotel
9. adultsNumber of adults
10. childrenNumber of children
11. babiesNumber of babies
12. mealType of meal booked. Categories are presented in standard hospitality meal packages: Undefined/SC – no meal package; BB – Bed & Breakfast; HB – Half board (breakfast and one other meal – usually dinner); FB – Full board (breakfast, lunch and dinner)
13. countryCountry of origin. Categories are represented in the ISO 3155–3:2013 format
14. market_segment Market segment designation. In categories, the term “TA” means “Travel Agents” and “TO” means “Tour Operators”
15. distribution_channel Booking distribution channel. The term “TA” means “Travel Agents” and “TO” means “Tour Operators”
16. is_repeated_guestValue indicating if the booking name was from a repeated guest (1) or not (0)
17. previous_cancellationsNumber of previous bookings that were cancelled by the customer prior to the current booking
18. previous_bookings_not_canceledNumber of previous bookings not cancelled by the customer prior to the current booking
19. reserved_room_typeCode of room type reserved. Code is presented instead of designation for anonymity reasons.
20. assigned_room_type
Code for the type of room assigned to the booking. Sometimes the assigned room type differs from the reserved room type due to hotel operation reasons (e.g. overbooking) or by customer request. Code is presented instead of designation for anonymity reasons.
21. booking_changes Number of changes/amendments made to the booking from the moment the booking was entered on the PMS until the moment of check-in or cancellation
22. deposit_type Indication on if the customer made a deposit to guarantee the booking. This variable can assume three categories: No Deposit – no deposit was made; Non Refund – a deposit was made in the value of the total stay cost; Refundable – a deposit was made with a value under the total cost of stay.
23. agentID of the travel agency that made the booking
24. companyID of the company/entity that made the booking or responsible for paying the booking. ID is presented instead of designation for anonymity reasons
25. days_in_waiting_list Number of days the booking was in the waiting list before it was confirmed to the customer
26. customer_type Type of booking, assuming one of four categories:
Contract - when the booking has an allotment or other type of contract associated to it; Group – when the booking is associated to a group; Transient – when the booking is not part of a group or contract, and is not associated to other 
27. transient booking; Transient-party – when the booking is transient, but is associated to at least other transient booking
adrAverage Daily Rate as defined by dividing the sum of all lodging transactions by the total number of staying nights
28. required_car_parking_spaces Number of car parking spaces required by the customer
29. total_of_special_requests Number of special requests made by the customer (e.g. twin bed or high floor)
30. reservation_status Reservation last status, assuming one of three categories: Canceled – booking was canceled by the customer; Check-Out – customer has checked in but already departed; No-Show – customer did not check-in and did inform the hotel of the reason why
31. reservation_status_date Date at which the last status was set. This variable can be used in conjunction with the ReservationStatus to understand when was the booking canceled or when did the customer checked-out of the hotel
</details>

## MindsDB Code example
```python
import mindsdb
import sys
import pandas as pd
from sklearn.metrics import balanced_accuracy_score


def run():
    backend = 'lightwood'

    mdb = mindsdb.Predictor(name='hotel_booking')

    mdb.learn(from_data='dataset/train.csv', to_predict='is_canceled', backend=backend,
              disable_optional_analysis=True)

    test_df = pd.read_csv('dataset/test.csv')
    predictions = mdb.predict(when_data='dataset/test.csv',
                              unstable_parameters_dict={'always_use_model_predictions': True})

    results = [str(x['is_canceled']) for x in predictions]
    real = list(map(str, list(test_df['is_canceled'])))

    accuracy = balanced_accuracy_score(real, results)

    #show additional info for each transaction row
    additional_info = [x.explanation for x in predictions]

    return {
        'accuracy': accuracy,
        'accuracy_function': 'balanced_accuracy_score',
        'backend': backend,
        'additional_info': additional_info
    }


if __name__ == '__main__':
    result = run()
    print(result)
```

## Mindsdb accuracy


| Accuraccy  | Backend  | Last run | MindsDB Version | Latest Version|
|----------------|-------------------|----------------------|-----------------|--------------|
| 0.8414694158197122, | Lightwood | 17 April 2020 | [![MindsDB](https://img.shields.io/badge/pypi--package-1.16.1-green)](https://pypi.org/project/MindsDB/1.16.1/)|   <a href="https://pypi.org/project/MindsDB/"><img src="https://badge.fury.io/py/MindsDB.svg" alt="PyPi Version"></a>|

<details>
  <summary>Click to expand MindsDB's explanation for first 1000 test rows:
  </summary>

```json
[{
    'accuracy': 0.8014610618674397,
    'accuracy_function': 'balanced_accuracy_score',
    'backend': 'lightwood',
    'additional_info': [{
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.8698,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.708,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9472,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.959,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9101,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.7314,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9605,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.8687,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.7358,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9326,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.691,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.6087,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.6193,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.8548,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9546,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.8079,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.8296,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9124,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.6073,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.7466,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9478,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9428,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.8331,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9501,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.6462,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9604,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9319,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.8954,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.8878,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9078,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.8689,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9593,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.926,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.8701,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9548,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9376,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9593,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.87,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9504,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9488,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.8912,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.8716,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.8992,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.6066,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9514,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9478,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9299,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.8127,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.8271,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9432,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9601,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9603,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.954,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9453,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.6762,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.938,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.935,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.8948,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9593,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9404,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9242,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9091,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.8686,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.7999,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9466,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9053,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.7565,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.8882,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.6166,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.6139,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.8569,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.6732,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.7506,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.6521,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9519,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9482,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.961,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9194,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.944,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9518,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.7449,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.8469,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.8995,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.7925,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.6266,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9054,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9451,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9545,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.8635,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.87,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.961,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9533,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': ['country']
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': ['country']
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.6155,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9551,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9491,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.7876,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9325,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9183,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.7374,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.8958,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9599,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9551,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9443,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9572,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9522,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9181,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.7177,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.7933,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.7531,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9338,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9595,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9505,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9315,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9044,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9183,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.6852,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9439,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.8819,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9534,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.7151,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9533,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9323,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9599,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.8796,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9189,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9476,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.8888,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9389,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.8923,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.948,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9537,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': ['country']
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': ['country']
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9096,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.6255,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9608,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.8994,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.7871,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.947,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9413,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.8203,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.834,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.8865,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9594,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.5976,
                    'explanation': {
                        'prediction_quality': 'somewhat confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'somewhat confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.6148,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.5974,
                    'explanation': {
                        'prediction_quality': 'somewhat confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'somewhat confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.7082,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9004,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.949,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.935,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9423,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.7573,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9014,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.8615,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.8222,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.902,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9448,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.8954,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9556,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9427,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9534,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.6863,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9304,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9222,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.7784,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9046,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9547,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.922,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.7719,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.6322,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.626,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.6927,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9477,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.784,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9606,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.8403,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9368,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.6816,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9606,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.7352,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.6504,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.5939,
                    'explanation': {
                        'prediction_quality': 'somewhat confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'somewhat confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9586,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9539,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.8792,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.8523,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9596,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.8002,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.7228,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.8897,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9289,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.8575,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9098,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9426,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.952,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9549,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.941,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9529,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9557,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.8161,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.8195,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.8392,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.7698,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.735,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9593,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.6702,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9605,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9582,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9468,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.7953,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.8207,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.8291,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9176,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9603,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.922,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.813,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9117,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9417,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9141,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9524,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9519,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9523,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9589,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9373,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9517,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.8075,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.7873,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9564,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.7009,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.96,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.8626,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.7925,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9551,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9549,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.7741,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9522,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.8766,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9451,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.953,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9607,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.64,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.5994,
                    'explanation': {
                        'prediction_quality': 'somewhat confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'somewhat confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.6651,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9609,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9531,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.895,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9602,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.841,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.5903,
                    'explanation': {
                        'prediction_quality': 'somewhat confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'somewhat confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.952,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9487,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9455,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9553,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.8507,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.7172,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9488,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.605,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9445,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.8792,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9431,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9592,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9509,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.8775,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.6124,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9599,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9502,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9596,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9582,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9531,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.6032,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.68,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.8792,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.903,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.6338,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.8689,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.6953,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9429,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9048,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9521,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9493,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.79,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.7176,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.7781,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9532,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9606,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9567,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.8645,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9526,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9127,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9594,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9549,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9514,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9574,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.8687,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9266,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.8855,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9527,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9549,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.6042,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9446,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9578,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.8798,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.7994,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.6952,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.5899,
                    'explanation': {
                        'prediction_quality': 'somewhat confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'somewhat confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9601,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.802,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9584,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9418,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.7698,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9594,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.8983,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9496,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.6122,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.6014,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9458,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9357,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.8969,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.8074,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.702,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9543,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.714,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9559,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.7847,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.6695,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9197,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9371,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9163,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9603,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9521,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9567,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9539,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9576,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.7018,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.8229,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.7701,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9541,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9455,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9173,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9159,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.8738,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.7715,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9604,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9334,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': ['country']
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': ['country']
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9555,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.7372,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9153,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9605,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.691,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.8936,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.6956,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9608,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9529,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9144,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9435,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9419,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9588,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.6556,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9187,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9407,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.7299,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.7451,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.7467,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9546,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.7425,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.6622,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.7833,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9608,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.6665,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.8206,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9532,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9602,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.6317,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.6801,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9605,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9255,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.6125,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9288,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9393,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.6125,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9501,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.8342,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9431,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9393,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.8248,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.953,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.6748,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9555,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.8989,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9523,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.7197,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9027,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.7975,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.9603,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.785,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '0',
                    'confidence': 0.9398,
                    'explanation': {
                        'prediction_quality': 'very confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'very confident',
                    'important_missing_information': []
                }
            }, {
                'is_canceled': {
                    'predicted_value': '1',
                    'confidence': 0.6859,
                    'explanation': {
                        'prediction_quality': 'confident',
                        'important_missing_information': []
                    },
                    'prediction_quality': 'confident',
                    'important_missing_information': []
                }
            }
]
```

</details>
