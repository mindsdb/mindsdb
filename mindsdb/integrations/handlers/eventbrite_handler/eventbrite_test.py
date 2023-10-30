from eventbrite.client import Client
client = Client(access_token="3HDF4GUMPE2XMLKBMICY")

# Or if you are using Oauth2 to get an access_token:

print(" ")
print(" ")
print(" ")
print("USER'S INFO")
me = client.get_current_user()
print(me)

print(" ")
print(" ")
print(" ")
print("GET USER ORGANIZATION INFO")
organizations = client.get_user_organizations()
print(organizations)

print(" ")
print(" ")
print(" ")
print("ORGANIZATION'S EVENTS")
events = client.list_events("1871338711793")
print(events)

# me_organization = client.get_user_organizations()
# print(me_organization)

print(" ")
print(" ")
print(" ")
print("EVENT DETAILS")
event = client.get_event("717926867587")
print(event)

print(" ")
print(" ")
print(" ")
print("ORGANIZATION'S VENUES")
venues = client.list_venues("1871338711793")
print(venues)

#{'name': {'text': 'AI Forum: Can AI Fix Climate Change?', 'html': 'AI Forum: Can AI Fix Climate Change?'}, 'description': {'text': "The third in a series of lunchtime presentations by King's researchers at Science Gallery London", 'html': "The third in a series of lunchtime presentations by King's researchers at Science Gallery London"}, 'url': 'https://www.eventbrite.co.uk/e/ai-forum-can-ai-fix-climate-change-tickets-717926867587', 'start': {'timezone': 'Europe/London', 'local': '2023-11-01T13:15:00', 'utc': '2023-11-01T13:15:00Z'}, 'end': {'timezone': 'Europe/London', 'local': '2023-11-01T14:00:00', 'utc': '2023-11-01T14:00:00Z'}, 'organization_id': '112948679745', 'created': '2023-09-12T15:38:12Z', 'changed': '2023-10-11T20:01:50Z', 'published': '2023-09-12T15:44:26Z', 'capacity': None, 'capacity_is_custom': None, 'status': 'live', 'currency': 'GBP', 'listed': True, 'shareable': True, 'online_event': False, 'tx_time_limit': 1200, 'hide_start_date': False, 'hide_end_date': False, 'locale': 'en_GB', 'is_locked': False, 'privacy_setting': 'unlocked', 'is_series': False, 'is_series_parent': False, 'inventory_type': 'limited', 'is_reserved_seating': False, 'show_pick_a_seat': False, 'show_seatmap_thumbnail': False, 'show_colors_in_seatmap_thumbnail': False, 'source': 'coyote', 'is_free': True, 'version': None, 'summary': "The third in a series of lunchtime presentations by King's researchers at Science Gallery London", 'facebook_event_id': None, 'logo_id': '596060689', 'organizer_id': '7201076133', 'venue_id': '173614819', 'category_id': '102', 'subcategory_id': None, 'format_id': '2', 'id': '717926867587', 'resource_uri': 'https://www.eventbriteapi.com/v3/events/717926867587/', 'is_externally_ticketed': False, 'logo': {'crop_mask': {'top_left': {'x': 0, 'y': 0}, 'width': 2160, 'height': 1080}, 'original': {'url': 'https://img.evbuc.com/https%3A%2F%2Fcdn.evbuc.com%2Fimages%2F596060689%2F112948679745%2F1%2Foriginal.20230912-154003?auto=format%2Ccompress&q=75&sharp=10&s=0bb5e81d009275e956f98c418a9a3025', 'width': 2160, 'height': 1080}, 'id': '596060689', 'url': 'https://img.evbuc.com/https%3A%2F%2Fcdn.evbuc.com%2Fimages%2F596060689%2F112948679745%2F1%2Foriginal.20230912-154003?h=200&w=450&auto=format%2Ccompress&q=75&sharp=10&rect=0%2C0%2C2160%2C1080&s=282ef80475d468edff954cb243435432', 'aspect_ratio': '2', 'edge_color': '#d6b4be', 'edge_color_set': True}}

print(" ")
print(" ")
print(" ")
print("CATEGORIES")
categories = client.list_categories()
print(categories)

print(" ")
print(" ")
print(" ")
print("SUBCATEGORIES")
subcategories = client.list_subcategories()
print(subcategories)

print(" ")
print(" ")
print(" ")
print("FORMATS")
formats = client.list_formats()
print(formats)