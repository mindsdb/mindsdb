from mindsdb.integrations.handlers.frappe_handler.frappe_handler import FrappeHandler
from mindsdb.integrations.handlers.gmail_handler.gmail_handler import GmailHandler

import os
import logging


class AgentToolFetcher:
    """
    This class is responsible for fetching the tools that can be used by an agent
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.handler_mapping = {
            "gmail": (GmailHandler, "gmail_token.json"),
            "frappe": (FrappeHandler, "frappe_token.txt", "frappe_domain.txt")
        }

    def get_token_path(self, base_dir, username, token_file):
        """
        Returns the path to the token file for a given user if it exists
        """
        token_path = os.path.join(base_dir, username, token_file)
        assert os.path.exists(token_path), f"Token file {token_path} for user {username} does not exist"
        return token_path

    def get_frappe_handler(self, base_tokens_dir, username_of_last_message):
        """
        Returns a FrappeHandler instance for a given user
        """
        frappe_token_path = self.get_token_path(base_tokens_dir, username_of_last_message, "frappe_token.txt")
        frappe_domain_path = self.get_token_path(base_tokens_dir, username_of_last_message, "frappe_domain.txt")
        return FrappeHandler(connection_data={
            "access_token": open(frappe_token_path, "r").read().strip(),
            "domain": open(frappe_domain_path, "r").read().strip()
        })

    def get_tools_for_agent(self, agent_name, base_tokens_dir, username_of_last_message):
        """
        Returns a list of tools that can be used by an agent
        """
        handler_class, *token_files = self.handler_mapping.get(agent_name, (None,))
        if not handler_class:
            self.logger.error(f"Unknown agent name {agent_name}")
            return []
        try:
            if agent_name == "gmail":
                gmail_token_path = self.get_token_path(base_tokens_dir, username_of_last_message, token_files[0])
                handler = GmailHandler()
                return handler.get_agent_tools(gmail_token_path)
            elif agent_name == "frappe":
                handler = self.get_frappe_handler(base_tokens_dir, username_of_last_message)
                return handler.get_agent_tools()
        except Exception as e:
            self.logger.error(f"Failed to get {agent_name} tools: {e}")
            return []
