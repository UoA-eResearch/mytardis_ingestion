# Mail server for forwarding logs

import logging
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

logger = logging.getLogger(__name__)

class MailHandler():

    def __init__(self,
                 config_dict):
        self.server = config_dict['server']
        self.port = config_dict['port']
        self.username = config_dict['username']
        self.password = config_dict['password']
        if 'receivers' in config_dict.keys():
            self.receivers = config_dict['receivers']
        else:
            self.receivers = ['c.seal@auckland.ac.nz']
        self.sender = f'myTardis_do_not_reply@auckland.ac.nz'

    def send_message(self,
                     subject,
                     message,
                     receiver = None,
                     sender = None):
        if not sender:
            sender = self.sender
        if not receiver:
            receiver = ', '.join(self.receivers)
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(host=self.server,
                              port = self.port,
                              context=context) as server:
            server.login(self.username,
                         self.password)
            mime_message = MIMEMultipart()
            mime_message['Subject'] = subject
            mime_message['From'] = sender
            mime_message['To'] = receiver
            text = MIMEText(message, "plain")
            mime_message.attach(text)
            result = server.sendmail(sender,
                            receiver,
                            mime_message.as_string())
            server.quit()

            return result
