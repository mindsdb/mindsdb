# TripAdvisor handler

Approved users of the Tripadvisor Content API can access the following business details for accommodations, restaurants, and attractions:

- Location ID, name, address, latitude & longitude
- Read reviews link, write-a-review link
- Overall rating, ranking, subratings, awards, the number of reviews the rating is based on, rating bubbles image
- Price level symbol, accommodation category/subcategory, attraction type, restaurant cuisine(s)

This integration contains 2 API References (will implement 3 more, need some checking before creating new tables):

## Find Search

The Location Search request returns up to 10 locations found by the given search query.
You can use category ("hotels", "attractions", "restaurants", "geos"), phone number, address, and latitude/longtitude to search with more accuracy.

```
SELECT *
FROM my_tripadvisor.searchLocationTable
WHERE searchQuery = 'New York' and category = 'hotels';
```

## Location Details

A Location Details request returns comprehensive information about a location (hotel, restaurant, or an attraction) such as name, address, rating, and URLs for the listing on Tripadvisor.

```
SELECT *
FROM my_tripadvisor.locationDetailsTable
WHERE locationId = '23322232';
```
