import imaplib
import email
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from datetime import datetime, timedelta

import pandas as pd
from mindsdb.integrations.handlers.email_handler.settings import EmailSearchOptions, EmailConnectionDetails
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class EmailClient:
    '''Class for searching emails using IMAP (Internet Messaging Access Protocol)'''

    _DEFAULT_SINCE_DAYS = 10

    def __init__(
            self,
            connection_data: EmailConnectionDetails
    ):
        self.email = connection_data.email
        self.password = connection_data.password
        self.imap_server = imaplib.IMAP4_SSL(connection_data.imap_server)
        self.smtp_server = smtplib.SMTP(connection_data.smtp_server, connection_data.smtp_port)

    def select_mailbox(self, mailbox: str = 'INBOX'):
        '''Logs in & selects a mailbox from IMAP server. Defaults to INBOX, which is the default inbox.

        Parameters:
            mailbox (str): The name of the mailbox to select.
        '''
        ok, resp = self.imap_server.login(self.email, self.password)
        if ok != 'OK':
            raise ValueError(
                f'Unable to login to mailbox {mailbox}. Please check your credentials: {str(resp)}')

        logger.info(f'Logged in to mailbox {mailbox}')

        ok, resp = self.imap_server.select(mailbox)
        if ok != 'OK':
            raise ValueError(
                f'Unable to select mailbox {mailbox}. Please check the mailbox name: {str(resp)}')

        logger.info(f'Selected mailbox {mailbox}')

    def logout(self):
        '''Shuts down the connection to the IMAP and SMTP server.'''

        try:
            ok, resp = self.imap_server.logout()
            if ok != 'BYE':
                logger.error(
                    f'Unable to logout of IMAP client: {str(resp)}')
            logger.info('Logged out of IMAP server')
        except Exception as e:
            logger.error(
                f'Exception occurred while logging out from IMAP server: {str(e)}')

        try:
            self.smtp_server.quit()
            logger.info('Logged out of SMTP server')
        except Exception as e:
            logger.error(
                f'Exception occurred while logging out from SMTP server: {str(e)}')

    def send_email(self, to_addr: str, subject: str, body: str):
        '''
        Sends an email to the given address.

        Parameters:
            to_addr (str): The email address to send the email to.
            subject (str): The subject of the email.
            body (str): The body of the email.
        '''

        msg = MIMEMultipart()
        msg['From'] = self.email
        msg['To'] = to_addr
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))

        self.smtp_server.starttls()
        self.smtp_server.login(self.email, self.password)
        self.smtp_server.send_message(msg)
        logger.info(f'Email sent to {to_addr} with subject: {subject}')

    def search_email(self, options: EmailSearchOptions) -> pd.DataFrame:
        '''Searches emails based on the given options and returns a DataFrame.

        Parameters:
            options (EmailSearchOptions): Options to use when searching using IMAP.

        Returns:
            df (pd.DataFrame): A dataframe of emails resulting from the search.
        '''
        self.select_mailbox(options.mailbox)

        try:

            query_parts = []
            if options.subject is not None:
                query_parts.append(f'(SUBJECT "{options.subject}")')

            if options.to_field is not None:
                query_parts.append(f'(TO "{options.to_field}")')

            if options.from_field is not None:
                query_parts.append(f'(FROM "{options.from_field}")')

            if options.since_date is not None:
                since_date_str = options.since_date.strftime('%d-%b-%Y')
            else:
                since_date = datetime.today() - timedelta(days=EmailClient._DEFAULT_SINCE_DAYS)
                since_date_str = since_date.strftime('%d-%b-%Y')
            query_parts.append(f'(SINCE "{since_date_str}")')

            if options.until_date is not None:
                until_date_str = options.until_date.strftime('%d-%b-%Y')
                query_parts.append(f'(BEFORE "{until_date_str}")')

            if options.since_email_id is not None:
                query_parts.append(f'(UID {options.since_email_id}:*)')

            query = ' '.join(query_parts)
            ret = []
            _, items = self.imap_server.uid('search', None, query)
            items = items[0].split()
            for emailid in items:
                _, data = self.imap_server.uid('fetch', emailid, '(RFC822)')
                email_message = email.message_from_bytes(data[0][1])

                email_line = {}
                email_line['id'] = emailid.decode()
                email_line['to_field'] = email_message.get('To')
                email_line['from_field'] = email_message.get('From')
                email_line['subject'] = email_message.get('Subject')
                email_line['date'] = email_message.get('Date')

                plain_payload = None
                html_payload = None
                content_type = 'html'
                for part in email_message.walk():
                    subtype = part.get_content_subtype()
                    if subtype == 'plain':
                        # Prioritize plain text payloads when present.
                        plain_payload = part.get_payload(decode=True)
                        content_type = 'plain'
                        break
                    if subtype == 'html':
                        html_payload = part.get_payload(decode=True)
                body = plain_payload or html_payload
                if body is None:
                    # Very rarely messages won't have plain text or html payloads.
                    continue
                email_line['body'] = plain_payload or html_payload
                email_line['body_content_type'] = content_type
                ret.append(email_line)
        except Exception as e:
            raise Exception('Error searching email') from e

        return pd.DataFrame(ret)
