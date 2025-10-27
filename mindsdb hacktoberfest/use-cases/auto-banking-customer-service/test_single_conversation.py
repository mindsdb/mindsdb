#!/usr/bin/env python3
import requests
import json

API_URL = "http://localhost:8000"

conversation = """client: Hi, I'm calling to inquire about donating to a local charity through Union Financial. Can you help me with that?
agent: Ofsolutely, Roseann! I have a few options for charating to charities through our bank. Can you tell me a little bit more about what you're looking to? Are you interested in making a one-time donation or setting up a recurring contribution?
client: Well, I'd like to make a one-time donation., but also set up a recurring monthly contributionation as Is that possible?
agent: Yes, definitely's definitely possible. We me walk you through our process real quick. First, we have a list of pre-approved charities that we work with. Would you like me to send that over to you via email?
client: That would be great, thank you!
agent: Great. I'll send that over right away. Once you've selected the charity you'd like to don, we can set up the donation. For a one-time donation, we can process that immediately. For the recurring monthly donation, we'll need to set up an automatic transfer from your Union Financial account to the charity's account. Is that sound good to you?
client: Yes, that sounds perfect. How do I go about selecting the charity?
agent: Like I mentioned earlier, we have a list of pre-approved charities that we work with. You can review that list and let me know which charity you'd like to support. If the charity you're interested in isn't on the list, we can still process the donation, it might take a little longer because we'll need to verify some additional information.
client: Okay, I see. I think I'd like to donate to the local animal shelter. They're not on the list, but I'm sure they're legitimate. Can we still donate to them?
agent: Absolutely! We can definitely still process the donation. the animal shelter. I'll just need to collect a bit more information from you to ensure everything goes smoothly. Can you please provide me with the charity's name and address?
client: Sure! The name of the shelter is PPaws and Claws" and their address is 123 Main Street.
agent: Perfect, I've got all the information I need. I'll go ahead and process the don-time donation and set up the recurring monthly transfer. Is there anything else I can assist you with today, Roseann?
client: No, that's all for now. Thank you so much for your help, Guadalupe!
agent: You're very welcome, Roseann! It was my pleasure to assist you. Just to summary, we've processed up a one-time donation to "aws and Claws Animal Shelter and a recurring monthly transfer to the same organization. Is there anything else I can do you with today?
client: Nope, that's it! Thanks again!
agent: You're welcome, Roseann. Have a wonderful day!"""


def test_conversation():
    try:
        response = requests.post(
            f"{API_URL}/api/process-conversations",
            json={"conversation_texts": [conversation]},
            timeout=90
        )

        if response.status_code == 200:
            result = response.json()

            if result['success'] and result['processed_count'] > 0:
                case = result['cases'][0]
                print(f"✓ Success - Status: {case['status']}")
                print(f"Summary: {case['summary']}")

                with open('test_result.json', 'w') as f:
                    json.dump(result, f, indent=2)
                return True
            else:
                print("✗ Failed - No conversations processed")
                return False
        else:
            print(f"✗ Failed - HTTP {response.status_code}")
            return False

    except requests.exceptions.Timeout:
        print("✗ Failed - Request timeout")
        return False
    except Exception as e:
        print(f"✗ Failed - {str(e)}")
        return False


if __name__ == "__main__":
    test_conversation()
