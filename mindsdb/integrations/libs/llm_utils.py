import re
import numpy as np


def get_completed_prompts(base_template, df):
    """
        Helper method that produces formatted prompts given a template and data in a Pandas DataFrame.
        It also returns the ID of any empty templates that failed to be filled due to missing data.
    """
    columns = []
    spans = []
    matches = list(re.finditer("{{(.*?)}}", base_template))

    assert len(matches) > 0, 'No placeholders found in the prompt, please provide a valid prompt template.'

    first_span = matches[0].start()
    last_span = matches[-1].end()

    for m in matches:
        columns.append(m[0].replace('{', '').replace('}', ''))
        spans.extend((m.start(), m.end()))

    spans = spans[1:-1]  # omit first and last, they are added separately
    template = [base_template[s:e] for s, e in
                list(zip(spans, spans[1:]))[::2]]  # take every other to skip placeholders  # noqa
    template.insert(0, base_template[0:first_span])  # add prompt start
    template.append(base_template[last_span:])  # add prompt end

    empty_prompt_ids = np.where(df[columns].isna().all(axis=1).values)[0]

    df['__mdb_prompt'] = ''
    for i in range(len(template)):
        atom = template[i]
        if i < len(columns):
            col = df[columns[i]].replace(to_replace=[None], value='')  # add empty quote if data is missing
            df['__mdb_prompt'] = df['__mdb_prompt'].apply(lambda x: x + atom) + col.astype("string")
        else:
            df['__mdb_prompt'] = df['__mdb_prompt'].apply(lambda x: x + atom)
    prompts = list(df['__mdb_prompt'])

    return prompts, empty_prompt_ids


# TODO: perhaps this should not be common. Move smaller clearly composable methods for LLM FT here.
def ft_jsonl_validation(
        items: list,  # read from a JSONL file
        messages_col: str = "messages",

        # valid keys for each chat message
        role_key: str = "role",
        content_key: str = "content",
        name_key: str = "name",

        # valid roles for each chat message
        system_key: str = "system",
        user_key: str = "user",
        assistant_key: str = "assistant",
):
    """
    This helper checks a list of dictionaries for compliance with the format usually expected by LLM providers
    (such as OpenAI or AnyscaleEndpoints) for fine-tuning LLMs that generate chat completions.
    
    Defaults for column names are set according to the expected defaults, but can be changed if needed by any given provider.
    
    :param items: list of JSON lines, each dictionary containing a chat sequence. Should be read from a JSONL file.
    :param messages_col: key in each dictionary to access a sequence of chat messages
    
    For each chat:
    :param role_key: key that defines the role of each message (e.g. system, user, or LLM)
    :param content_key: key that defines the content of each message
    :param name_key: key that defines the name of each message
    
    For each message:
    :param system_key: valid role for each chat message
    :param user_key: valid role for each chat message
    :param assistant_key: valid role for each chat message
    
    :return: None, raises an Exception if validation fails.
    """  # noqa
    valid_keys = (role_key, content_key, name_key)
    valid_roles = (system_key, user_key, assistant_key)
    try:
        for line_num, batch in enumerate(items):
            prefix = f"Error in line #{line_num + 1}: "
            if not isinstance(batch, dict):
                raise Exception(f"{prefix}Each line in the provided data should be a dictionary")

            if messages_col not in batch:
                raise Exception(f"{prefix}Each line in the provided data should have a '{messages_col}' key")

            if not isinstance(batch[messages_col], list):
                raise Exception(f"{prefix}Each line in the provided data should have a '{messages_col}' key with a list of messages")  # noqa

            messages = batch[messages_col]
            if not any(message.get(role_key, None) == assistant_key for message in messages):
                raise Exception(f"{prefix}Each message list should have at least one message with role '{assistant_key}'")  # noqa

            for message_num, message in enumerate(messages):
                prefix = f"Error in line #{line_num + 1}, message #{message_num + 1}: "
                if role_key not in message or content_key not in message:
                    raise Exception(f"{prefix}Each message should have a '{role_key}' and '{content_key}' key")

                if any(k not in valid_keys for k in message):
                    raise Exception(f"{prefix}Each message should only have these keys: {valid_keys}")

                if message.get("role", None) not in valid_roles:
                    raise Exception(f"{prefix}Each message should have a valid role (one out of {valid_roles})")

    except Exception as e:
        raise Exception(f"Fine-tuning data format is not valid. Got: {e}")
