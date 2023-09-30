# TripAdvisor handler #5369

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

## Description

Please include a summary of the change and the issue it solves.

Fixes #issue_number

## Type of change

(Please delete options that are not relevant)

- [ ] 🐛 Bug fix (non-breaking change which fixes an issue)
- [ ] ⚡ New feature (non-breaking change which adds functionality)
- [ ] 📢 Breaking change (fix or feature that would cause existing functionality not to work as expected)
- [ ] 📄 This change requires a documentation update

## Verification Process

To ensure the changes are working as expected:

- [ ] Test Location: Specify the URL or path for testing.
- [ ] Verification Steps: Outline the steps or queries needed to validate the change. Include any data, configurations, or actions required to reproduce or see the new functionality.

## Additional Media:

- [ ] I have attached a brief loom video or screenshots showcasing the new functionality or change.

## Checklist:

- [ ] My code follows the style guidelines(PEP 8) of MindsDB.
- [ ] I have appropriately commented on my code, especially in complex areas.
- [ ] Necessary documentation updates are either made or tracked in issues.
- [ ] Relevant unit and integration tests are updated or added.
