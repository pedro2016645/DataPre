"""
Script to send alerts by email
"""

import win32com.client as win32
import psutil
import os
import subprocess
import logging

logger = logging.getLogger(__name__)


class OutlookApp:
    """
    Class to control local outlook app to send emails
    """
    def __init__(self, subject: str, html_data: str, emails_to: list, attachments: list = None,
                 app_path: str = None, attachments_path_folder: str = None) -> None:
        """
        Constructor for the class to send emails though outlook
        :param subject: subject of the email
        :param html_data: template html in string
        :param attachments: list of attachments
        :param emails_to: list of emails to send
        :param app_path: path for outlook app
        :param attachments_path_folder: path to the attachments folder
        """

        # init variables
        self._process_call = None
        self._attachments_path_folder = None
        self._subject = None
        self._html_data = None
        self._attachments = None
        self._emails_to = None

        try:
            if app_path is None:
                app_path = r'C:\Program Files (x86)\Microsoft Office\Office15\OUTLOOK.exe'
            if attachments_path_folder is not None:
                assert type(attachments_path_folder) == str, "The attachments_path_folder must be a string"
                assert os.path.exists(attachments_path_folder), "The attachments_path_folder: {} does not exist".format(
                    attachments_path_folder)

            assert type(app_path) == str, "The app_path must be a string"
            assert os.path.exists(app_path), "The app_path: {} does not exist".format(app_path)
            assert type(subject) == str, "The subject must be a string"
            assert type(html_data) == str, "The html_data must be a string"

            if attachments is None:
                attachments = []
            assert type(attachments) == list, "The attachments must be a list"

            # Check if all attachments exist
            verification_list = [x for x in attachments if not os.path.exists(os.path.join(attachments_path_folder, x))]
            assert len(verification_list) == 0, "The following attachments doesn't exist:\n{}".format("\n".join(
                verification_list))
            assert type(emails_to) == list, "The emails_to must be a string"

            self._process_call = app_path
            self._subject = subject
            self._html_data = html_data
            self._attachments_path_folder = attachments_path_folder
            self._attachments = attachments
            self._emails_to = ";".join(emails_to)
            logger.info("Setting outlook call: OK")

        except Exception as e:
            logger.error("Error on initializing object: {}".format(e))
            raise Exception(e)

    def __open(self) -> None:
        """
        Function to open the app on the local computer in order to send the email
        """
        try:

            subprocess.call([self._process_call])
            logger.info("Successfully opened outlook")

        except Exception as e:
            logger.error(e)
            raise Exception(e)

    def __send_email(self) -> None:
        """
        Function that writes and send emails on Outlook
        """
        logging.info("Building email to send")
        outlook = win32.Dispatch('outlook.application')
        ol_format_rich_text = 3
        ol_mail_item = 0x0

        # writing new email
        new_email = outlook.CreateItem(ol_mail_item)
        new_email.Subject = self._subject
        new_email.BodyFormat = ol_format_rich_text
        new_email.HTMLBody = self._html_data
        new_email.To = self._emails_to

        # Add attachments
        for source in self._attachments:
            new_email.Attachments.Add(
                Source=os.path.join(self._attachments_path_folder, source))

        # Send Email
        new_email.Send()

    def send_new_alert(self) -> None:
        """
        Send the new email depending if the app is open or not
        """

        # Create bool to check if the app is open or not
        self.__send_email()
