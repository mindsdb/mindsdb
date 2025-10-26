#!/usr/bin/env python3
"""Test script for the recommendation workflow."""

import json
import requests
from typing import Dict, Any


def test_recommendation_workflow() -> None:
    """Test the complete recommendation workflow with an unresolved conversation."""

    # Test conversation that should be classified as UNRESOLVED
    conversation = """agent: Thank you for calling FirstCity Bank, this is Maria. How can I help you today?
client: Hi Maria, I just got an email about some "DD bonus," and I have no idea what that means. Is this even from your bank or is it a scam?
agent: I can understand why that sounds confusing. "DD" just stands for Direct Deposit—it's our shorthand in internal documents, but it shouldn't have been sent out like that.
client: Well, it sure looks suspicious. The email says I'll get a "DD bonus if I set up recurring DD by the 25th." That could mean anything.
agent: You're right. It should've said Direct Deposit bonus. The offer is legitimate—it gives you $200 if your paycheck or benefits are automatically deposited into your account twice by the 25th.
client: So it's not some weird crypto thing or data transfer code?
agent: No, definitely not. Just our marketing team being too fond of acronyms.
client: Alright, tell them to spell things out next time. Banks should know better than to sound like hackers.
agent: Message received. I'll make sure your feedback gets passed along, and you're all set for the bonus once your Direct Deposit is active.
client: Fine, thanks. And tell them "DD" stands for Don't Do that again.
agent: Noted! Have a great day, Linda."""

    print("🧪 Testing Recommendation Workflow")
    print("=" * 50)

    # Test the conversation processing
    try:
        response = requests.post(
            "http://localhost:8000/api/process-conversations",
            json={"conversation_texts": [conversation]},
            timeout=30
        )

        if response.status_code == 200:
            result = response.json()
            print("✅ Conversation processed successfully!")
            print(f"📊 Processed: {result['processed_count']}/{result['total_conversations']} conversations")
            print(f"⏱️  Processing time: {result['processing_time_seconds']}s")

            # Get the first case from the response
            if result.get('cases') and len(result['cases']) > 0:
                case = result['cases'][0]
                print(f"\n📋 Conversation ID: {case['conversation_id']}")
                print(f"📝 Summary: {case['summary']}")
                print(f"🔍 Status: {case['status']}")

                # Check if recommendation was generated
                if case.get('recommendation'):
                    print(f"\n💡 AI Recommendation:\n{case['recommendation']}")
                else:
                    print("\n⚠️  No recommendation generated")
                    if case.get('recommendation_error'):
                        print(f"❌ Recommendation Error: {case['recommendation_error']}")

                # Check Jira integration
                if case.get('jira_issue_key'):
                    print(f"\n🎫 Jira Issue: {case['jira_issue_key']}")
                    print(f"🔗 Jira URL: {case['jira_issue_url']}")
                else:
                    print("\n⚠️  No Jira issue created")
                    if case.get('jira_issue_error'):
                        print(f"❌ Jira Error: {case['jira_issue_error']}")

                # Check Salesforce integration
                if case.get('salesforce_case_id'):
                    print(f"\n📞 Salesforce Case: {case['salesforce_case_id']}")
                    print(f"🔗 Salesforce URL: {case['salesforce_case_url']}")
                else:
                    print("\n⚠️  No Salesforce case created")
                    if case.get('salesforce_error'):
                        print(f"❌ Salesforce Error: {case['salesforce_error']}")

                # Save result to file
                with open("recommendation_test_result.json", "w") as f:
                    json.dump(result, f, indent=2)
                print("\n💾 Full result saved to recommendation_test_result.json")
            else:
                print("⚠️  No cases returned in response")

        else:
            print(f"❌ Error processing conversation: {response.status_code}")
            print(f"Response: {response.text}")

    except requests.exceptions.RequestException as exc:
        print(f"❌ Request failed: {exc}")
        print("Make sure the server is running on http://localhost:8000")
    except Exception as exc:
        print(f"❌ Unexpected error: {exc}")


if __name__ == "__main__":
    test_recommendation_workflow()