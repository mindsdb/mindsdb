# TripAdvisor handler #5369

Custom Python Wrapper: tripadvisor_api.py

This handler integrates with the [TripAdvisor API](https://tripadvisor-content-api.readme.io/reference/overview). This integration will enable TripAdvisor users to use ML to research travel destinations and more. The custom Python wrapper had to be made which can be found in the **tripadvisor_api.py**.

Approved users of the Tripadvisor Content API can access the following business details for accommodations, restaurants, and attractions:

- Location ID, name, address, latitude & longitude
- Read reviews link, write-a-review link
- Overall rating, ranking, subratings, awards, the number of reviews the rating is based on, rating bubbles image
- Price level symbol, accommodation category/subcategory, attraction type, restaurant cuisine(s)

# Connecting to TripAdvisor and Calling APIs

CREATE DATABASE my_tripadvisor
WITH
ENGINE = 'tripadvisor'
PARAMETERS = {'api_key': 'INSERT YOUR API KEY'};

This is how you start the connection to TripAdvisor API via MindsDB.

## Find Search API Reference

The Location Search request returns up to 10 locations found by the given search query.
You can use category ("hotels", "attractions", "restaurants", "geos"), phone number, address, and latitude/longtitude to search with more accuracy.

```
SELECT *
FROM my_tripadvisor.searchLocationTable
WHERE searchQuery = 'New York';
```

The details of this API reference can be found here: https://tripadvisor-content-api.readme.io/reference/searchforlocations

## Location Details API reference

A Location Details request returns comprehensive information about a location (hotel, restaurant, or an attraction) such as name, address, rating, and URLs for the listing on Tripadvisor.

```
SELECT *
FROM my_tripadvisor.locationDetailsTable
WHERE locationId = '23322232';
```

The details of this API reference can be found here: https://tripadvisor-content-api.readme.io/reference/getlocationdetails

## Location Reviews API reference

The Location Reviews request returns up to 5 of the most recent reviews for a specific location.

```
SELECT *
FROM my_tripadvisor.reviewsTable
WHERE locationId = '99288';
```

The details of this API reference can be found here: https://tripadvisor-content-api.readme.io/reference/getlocationreviews

## Location Photos API reference

The Location Reviews request returns up to 5 of the most recent reviews for a specific location.

```
SELECT *
FROM my_tripadvisor.photosTable
WHERE location_id = '99288';
```

The details of this API reference can be found here: https://tripadvisor-content-api.readme.io/reference/getlocationphotos

## Location Nearby API reference

The Location Reviews request returns up to 5 of the most recent reviews for a specific location.

```
SELECT *
FROM my_tripadvisor.nearbyLocationTable
WHERE latLong = '40.780825, -73.972781';
```

The details of this API reference can be found here: https://tripadvisor-content-api.readme.io/reference/searchfornearbylocations

# Use Case: Sentiment Analysis of Reviews from TripAdvisor with OpenAI

## Search Hotels in New York and Choose one

We will search for the hotels in New York in the area around the given longitude and latitude. If we execute the
SQL script then we will get the following result which is displayed below the SQL script. We will choose
"Arthouse Hotel New York City" to see the reviews.

```
SELECT *
FROM my_tripadvisor.searchLocationTable
WHERE searchQuery='New York' and latLong='40.780825, -73.972781' and category='hotels';
```

### Result:

| location_id | name                         | distance           | rating | bearing | street1                           | street2 | city          | state    | country       | postalcode | address_string                                                  | phone  | latitude | longitude |
| ----------- | ---------------------------- | ------------------ | ------ | ------- | --------------------------------- | ------- | ------------- | -------- | ------------- | ---------- | --------------------------------------------------------------- | ------ | -------- | --------- |
| **99288**   | Arthouse Hotel New York City | 0.4528974982904253 | [NULL] | west    | 2178 Broadway at West 77th Street | [NULL]  | New York City | New York | United States | 10024-6647 | 2178 Broadway at West 77th Street, New York City, NY 10024-6647 | [NULL] | [NULL]   | [NULL]    |
| 112064      | Warwick New York             | 1.300511583690564  | [NULL] | south   | 65 W 54th St                      |         | New York City | New York | United States | 10019      | 65 W 54th St, New York City, NY 10019                           | [NULL] | [NULL]   | [NULL]    |
| 611947      | New York Hilton Midtown      | 1.3284012949631072 | [NULL] | south   | 1335 Avenue Of The Americas       | [NULL]  | New York City | New York | United States | 10019-6078 | 1335 Avenue Of The Americas, New York City, NY 10019-6078       | [NULL] | [NULL]   | [NULL]    |

.
.
.

## Creating the OpenAI model

Creating the model and the prompt.

```
CREATE MODEL sentiment_classifier_gpt_tripadvisor
PREDICT sentiment
USING
engine = 'openai',
prompt_template = 'describe the sentiment of the reviews
strictly as "positive", "neutral", or "negative".
"I love the product":positive
"It is a scam":negative
"{{review}}.":',
api_key='Your API KEY';
```

## Perform sentiment analysis

```
SELECT input.text_review, output.sentiment
FROM my_tripadvisor.reviewsTable AS input
JOIN sentiment_classifier_gpt_tripadvisor AS output
WHERE input.locationId = '99288';
```

### Result:

| text_review | sentiment |
| ----------- | --------- |

| Stayed here numerous times over the years and loved the hotel. This visit was disappointing from start to finish. No hand soap in room and told to go to CVS to purchase some. Rude staff at check-in. Don't know what happened to the once best hotel on the Upper West! | negative |

| Physically the room was appalling. The undersized bathroom you could not actually close the door without squeezing in between the metal (?) vanity and toilet. No chair or table nor room to do any exercising or stretching. You will need to stretch to be able to close the bath door and open the shower door. Most hotels will have auto close room doors balance so they auto lock. Not the Arthouse... here you better make sure you hear the click on the lock which takes considerable force. The poor balancing will leave your room wide open. The staff is courteous but wonder about their veracity as they told us our rooms were "upgraded" to a king room. If this was an upgrade ....sheesh hate to have seen the original room. No mention was made of the "urban fee" which provided dicey wifi with a deceptive title. The SeraFina restaurant was disappointing ans it appears wo be more rundown than the hotel with cracked vinyl benches for seating with average Italian food at best. NYC has many fine boutique Hotels..... this isn't one of them. | negative |

| Hotel is just ok at best and is incredibly strict about cancellation policies. I would avoid booking here. It is not nice enough to warrant choosing this over another hotel in the area with better customer service. | negative |

| Although we have stayed here before, this was a horrible experience! I booked the Broadway Deluxe King Room in the picture which looks very comfortable but they gave us a room 1/3 smaller and claimed it was the "same category" as the one I booked. We've stayed in the room before and it was quite a good size, had an extra chair, good sized closet and bathroom. This room was tiny, had no extra chair and the bathroom was so small one could barely turn around in it. When I complained to the manager he gave me a credit of the "resort fee" and said the exact line of rooms in the picture wasn't available. They shouldn't show a picture of a room on the website that is substantially different than the one a guest stays in.

In addition, the shade in the room was broken, it took 3 requests over 2 days to get a few extra towels and there wasn't hot (only lukewarm) water for the first two days of our stay despite multiple complaints.

I would avoid this hotel in the future. | negative |

| We were in the neighborhood and dropped into Arthouse's lobby bar with live music. We ended up staying for a few hours and had an absolutely lovely time. Shout out to our server Elviz for the quick service and to the piano player Greg for a memorable night! The best music we've heard in a long time. Lovely hotel and would highly recommend! We will be back | positive |
