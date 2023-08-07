import smtplib
import imaplib
import email
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pandas as pd


class EmailClient:
    def __init__(self, email, password, smtp_server='smtp.gmail.com', smtp_port=587, imap_server="imap.gmail.com"):
        self.email = email
        self.password = password
        self.smtp_server = smtplib.SMTP(smtp_server, smtp_port)
        self.imap_server = imaplib.IMAP4_SSL(imap_server)

    def send_email(self, to_addr, subject, body):
        msg = MIMEMultipart()
        msg['From'] = self.email
        msg['To'] = to_addr
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))

        self.smtp_server.starttls()
        self.smtp_server.login(self.email, self.password)
        self.smtp_server.send_message(msg)
        self.smtp_server.quit()

    def search_email(self, mailbox="INBOX", subject=None, to=None, from_=None, since_date=None, until_date=None,
                     since_emailid=None):

        self.imap_server.login(self.email, self.password)

        self.imap_server.select(mailbox)

        query_parts = []
        if subject is not None:
            query_parts.append(f'(SUBJECT "{subject}")')
        if to is not None:
            query_parts.append(f'(TO "{to}")')
        if from_ is not None:
            query_parts.append(f'(FROM "{from_}")')
        if since_date is not None:
            since_date_str = since_date.strftime("%d-%b-%Y")
            query_parts.append(f'(SINCE "{since_date_str}")')
        if until_date is not None:
            until_date_str = until_date.strftime("%d-%b-%Y")
            query_parts.append(f'(BEFORE "{until_date_str}")')
        if since_emailid is not None:
            query_parts.append(f'(UID {since_emailid}:*)')

        query = ' '.join(query_parts)

        ret = []

        resp, items = self.imap_server.uid('search', None, query)
        items = items[0].split()
        for emailid in items[::-1]:
            resp, data = self.imap_server.uid('fetch', emailid, "(BODY[HEADER.FIELDS (SUBJECT TO FROM DATE)])")
            try:
                raw_email = data[0][1].decode("utf-8")
            except UnicodeDecodeError:
                ValueError(f"Could not decode email with id {emailid}")
            email_message = email.message_from_string(raw_email)

            email_line = {}
            email_line['id'] = emailid
            email_line["to"] = email_message['To']
            email_line["from"] = email_message['From']
            email_line["subject"] = str(email_message['Subject'])
            email_line["created_at"] = email_message['Date']
            resp, email_data = self.imap_server.uid('fetch', emailid, '(BODY[TEXT])')
            email_line["body"] = email_data[0][1].decode('utf-8')

            ret.append(email_line)

        self.imap_server.logout()

        return pd.DataFrame(ret)




