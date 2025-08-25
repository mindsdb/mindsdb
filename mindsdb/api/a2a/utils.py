from typing import Dict, List
from mindsdb.utilities.log import getLogger

logger = getLogger(__name__)


def to_serializable(obj):
    # Primitives
    if isinstance(obj, (str, int, float, bool, type(None))):
        return obj
    # Pydantic v2
    if hasattr(obj, "model_dump"):
        return to_serializable(obj.model_dump(exclude_none=True))
    # Pydantic v1
    if hasattr(obj, "dict"):
        return to_serializable(obj.dict(exclude_none=True))
    # Custom classes with __dict__
    if hasattr(obj, "__dict__"):
        return {k: to_serializable(v) for k, v in vars(obj).items() if not k.startswith("_")}
    # Dicts
    if isinstance(obj, dict):
        return {k: to_serializable(v) for k, v in obj.items()}
    # Lists, Tuples, Sets
    if isinstance(obj, (list, tuple, set)):
        return [to_serializable(v) for v in obj]
    # Fallback: string
    return str(obj)


def convert_a2a_message_to_qa_format(a2a_message: Dict) -> List[Dict[str, str]]:
    """
    Convert A2A message format to question/answer format.

    This is the format that the langchain agent expects and ensure effective multi-turn conversation

    Args:
        a2a_message: A2A message containing history and current message parts

    Returns:
        List of messages in question/answer format
    """
    converted_messages = []

    # Process conversation history first
    if "history" in a2a_message:
        for hist_msg in a2a_message["history"]:
            if hist_msg.get("role") == "user":
                # Extract text from parts
                text = ""
                for part in hist_msg.get("parts", []):
                    if part.get("type") == "text":
                        text = part.get("text", "")
                        break
                # Create question with empty answer initially
                converted_messages.append({"question": text, "answer": ""})
            elif hist_msg.get("role") in ["agent", "assistant"]:
                # Extract text from parts
                text = ""
                for part in hist_msg.get("parts", []):
                    if part.get("type") == "text":
                        text = part.get("text", "")
                        break
                # Pair with the most recent question that has empty answer
                paired = False
                for i in range(len(converted_messages) - 1, -1, -1):
                    if converted_messages[i].get("answer") == "":
                        converted_messages[i]["answer"] = text
                        paired = True
                        break

                if not paired:
                    logger.warning("Could not pair agent response with question (no empty answer found)")

        logger.debug(f"Converted {len(a2a_message['history'])} A2A history messages to Q&A format")

    # Add current message as final question with empty answer
    current_text = ""
    for part in a2a_message.get("parts", []):
        if part.get("type") == "text":
            current_text = part.get("text", "")
            break
    converted_messages.append({"question": current_text, "answer": ""})

    return converted_messages
