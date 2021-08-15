"""
Module to setup and build messages and alerts for each Pipeline
"""
import os
import logging
from ..alerts.emails import OutlookApp
from datetime import datetime
logger = logging.getLogger(__name__)


class ClientDeliver:
    """
    Class to build the House Early Liquidation Alerts
    """
    @staticmethod
    def receipt(file_name: str, folder: str, date_obj: datetime, **notification_params) -> None:
        """
        Function that sends emails to the business with the calculated metrics
        :params configurations: includes information from a yaml configuration file in order to specify what
        scenario are we in
        """

        try:
            assert 'client' in notification_params.keys(), "The client is not defined"
            client_notification_params = notification_params['client']
            assert 'template' in client_notification_params.keys(), "The template is not defined"
            template_path = client_notification_params['template']
            assert os.path.exists(template_path), "The template path; {} is not correct".format(template_path)
            # Prepare info
            logger.info('Preparing the info for the html')

            # Load html
            html_data = open(template_path, 'r', encoding='utf-8').read()

            assert 'alert' in client_notification_params.keys()
            html_alert_params = client_notification_params['alert']
            html_alert_params['file_name'] = file_name
            html_alert_params['file_link'] = os.path.join(html_alert_params['rep_link'], folder, file_name)
            assert 'time_format' in html_alert_params.keys()

            html_alert_params['time_reference'] = date_obj.strftime(html_alert_params['time_format'])
            for k in html_alert_params.keys():
                html_data = html_data.replace('[{}]'.format(k), html_alert_params[k])

            logger.info('Html metrics replaced')
            assert 'subject' in client_notification_params.keys()
            assert 'emails_to' in client_notification_params.keys()
            # Get attachment folder
            # Build email to send
            OutlookApp(subject=client_notification_params['subject'],
                       html_data=html_data,
                       emails_to=client_notification_params['emails_to'],
                       app_path=None).send_new_alert()

        except Exception as e:
            logger.error(e)
            raise Exception(e)
