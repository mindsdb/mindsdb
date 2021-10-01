---
id: booking-cancellation
title: Hotel Booking Demand 
---

| Industry       | Department | Role               |
|----------------|------------|--------------------|
| Travel | Operations | Business executive |

## Processed Dataset 

###### [![Data](https://img.shields.io/badge/GET--DATA-HotelBooking-green)](https://github.com/mindsdb/mindsdb-examples/tree/master/classics/hotel-booking/dataset)

This [data set](https://www.kaggle.com/jessemostipak/hotel-booking-demand) contains booking information for a city hotel and a resort hotel, and includes information such as when the booking was made, length of stay, the number of adults, children, and/or babies, and the number of available parking spaces, among other things.

{{ read_csv('https://raw.githubusercontent.com/mindsdb/mindsdb-examples/master/classics/hotel-booking/raw_data/hotel_bookings.csv', nrows=7) }}


<details>
  <summary>Click to expand Features Informations:</summary>

```
1. hotel Hotel (H1 = Resort Hotel or H2 = City Hotel)
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
```

</details>

## MindsDB Code example
```python
import mindsdb_native
import sys
import pandas as pd
from sklearn.metrics import balanced_accuracy_score


def run():

    mdb = mindsdb_native.Predictor(name='hotel_booking')

    mdb.learn(from_data='processed_data/train.csv', to_predict='is_canceled')

    test_df = pd.read_csv('processed_data/test.csv')
    predictions = mdb.predict(when_data='processed_data/test.csv')

    results = [str(x['is_canceled']) for x in predictions]
    real = list(map(str, list(test_df['is_canceled'])))

    accuracy = balanced_accuracy_score(real, results)

    #show additional info for each transaction row
    additional_info = [x.explanation for x in predictions]

    return {
        'accuracy': accuracy,
        'accuracy_function': 'balanced_accuracy_score',
        'additional_info': additional_info
    }


if __name__ == '__main__':
    result = run()
    print(result)
```

## Mindsdb accuracy


| Accuracy  | Backend  | Last run | MindsDB Version | Latest Version|
|----------------|-------------------|----------------------|-----------------|--------------|
| 0.8414694158197122, | Lightwood | 17 April 2020 | [![MindsDB](https://img.shields.io/badge/pypi--package-1.16.1-green)](https://pypi.org/project/MindsDB/1.16.1/)|   <a href="https://pypi.org/project/MindsDB/"><img src="https://badge.fury.io/py/MindsDB.svg" alt="PyPi Version"></a>|

