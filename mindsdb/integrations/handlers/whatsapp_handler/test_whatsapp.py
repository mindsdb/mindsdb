import os
from twilio.rest import Client


# Find your Account SID and Auth Token at twilio.com/console
# and set the environment variables. See http://twil.io/secure
account_sid = "AC37910d1a308d0c53d1dc9b7f1f352a34"
auth_token  = "5ab63e11d89aec875d5a212af1e7223e"
client = Client(account_sid, auth_token)

message = client.messages.create(
            body='I am doing good, how about you?',
            from_='whatsapp:+14155238886',
            to='whatsapp:+15519990863'
        )

print(message.sid)
print(message.body)
print(message.status)
print(message.from_)
print(message.to)
