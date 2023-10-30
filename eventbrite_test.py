from eventbrite.client import Client

client = Client(access_token="3HDF4GUMPE2XMLKBMICY")

# event = client.get_event("717926867587")
# print(event)

categories = client.list_categories()
print(categories)
