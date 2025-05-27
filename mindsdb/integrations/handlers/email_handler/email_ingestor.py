import re

from bs4 import BeautifulSoup
import bs4.element
import chardet

import pandas as pd

from mindsdb.integrations.handlers.email_handler.email_client import EmailClient
from mindsdb.integrations.handlers.email_handler.settings import EmailSearchOptions


class EmailIngestor:
    """
    Parses emails into a DataFrame.
    Does some preprocessing on the raw HTML to extract meaningful text.
    """

    def __init__(self, email_client: EmailClient, search_options: EmailSearchOptions):
        self.email_client = email_client
        self.search_options = search_options

    def _is_tag_visible(self, element):
        if element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']:
            return False
        if isinstance(element, bs4.element.Comment):
            return False
        return True

    def _preprocess_raw_html(self, html: str) -> str:
        soup = BeautifulSoup(html, 'html.parser')
        texts = soup.find_all(text=True)
        visible_texts = filter(self._is_tag_visible, texts)
        return '\n'.join(t.strip() for t in visible_texts)

    def _ingest_email_row(self, row: pd.Series) -> dict:
        if row['body_content_type'] == 'html':
            # Extract meaningful text from raw HTML.
            row['body'] = self._preprocess_raw_html(row['body'])
        body_str = row['body']
        encoding = None
        if isinstance(body_str, bytes):
            encoding = chardet.detect(body_str)['encoding']
            if 'windows' in encoding.lower():
                # Easier to treat this at utf-8 since str constructor doesn't support all encodings here:
                # https://chardet.readthedocs.io/en/latest/supported-encodings.html.
                encoding = 'utf-8'
            try:
                body_str = str(body_str, encoding=encoding)
            except UnicodeDecodeError:
                # If illegal characters are found, we ignore them.
                # I encountered this issue with some emails that had a mix of encodings.
                body_str = row['body'].decode(encoding, errors='ignore')
        # We split by paragraph so make sure there aren't too many newlines in a row.
        body_str = re.sub(r'[\r\n]\s*[\r\n]', '\n\n', body_str)
        email_data = {
            'id': row['id'],
            'body': body_str,
            'subject': row['subject'],
            'to_field': row['to_field'],
            'from_field': row['from_field'],
            'datetime': row['date']
        }
        # Replacing None values {None: ""}
        for key in email_data:
            if email_data[key] is None:
                email_data[key] = ""

        return email_data

    def ingest(self) -> pd.DataFrame:
        emails_df = self.email_client.search_email(self.search_options)
        all_email_data = []
        for _, row in emails_df.iterrows():
            all_email_data.append(self._ingest_email_row(row))

        df = pd.DataFrame(all_email_data)

        # Replace "(UTC)" with empty string over a pandas DataFrame column
        if 'datetime' in df.columns:
            df['datetime'] = df['datetime'].str.replace(' (UTC)', '')

            # Convert datetime string to datetime object, and normalize timezone to UTC.
            df['datetime'] = pd.to_datetime(df['datetime'], utc=True, format="%a, %d %b %Y %H:%M:%S %z", errors='coerce')

        return df
