import smtplib
import imaplib
import email
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.header import decode_header

class EmailClient:
    def __init__(self, email, password, smptp_server = 'smpt.gmail.com', smpt_port = 587, imap_server="imap.gmail.com"):
        self.email = email
        self.password = password
        self.smtp_server = smtplib.SMTP(smptp_server, smpt_port)
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

    def search_emails(self, mailbox="inbox", subject=None):
        self.imap_server.login(self.email, self.password)
        self.imap_server.select(mailbox)

        if subject is not None:
            result, data = self.imap_server.uid('search', None, f'(HEADER Subject "{subject}")')
        else:
            result, data = self.imap_server.uid('search', None, "ALL")
        
        email_ids = data[0].split()
        for num in email_ids:
            result, data = self.imap_server.uid('fetch', num, '(BODY[HEADER.FIELDS (SUBJECT)])')
            raw_email = data[0][1].decode("utf-8")
            email_message = email.message_from_string(raw_email)

            subject = decode_header(email_message['Subject'])[0][0]
            if isinstance(subject, bytes):
                subject = subject.decode()
            print('Subject:', subject)
        self.imap_server.logout()


if __name__ == "__main__":
    email_client = EmailClient()