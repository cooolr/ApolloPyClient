# -*- coding: utf-8 -*-
"""
Configuring the Apollo Python3 Client
https://github.com/ctripcorp/apollo/wiki/其它语言客户端接入指南
"""

import os
import json
import time
import doctest
import logging
import requests
from urllib.parse import quote
from threading import _start_new_thread


class ApolloPyClient:
    """
    Configuring the Apollo Python3 Client
    usage:
    
    # No configuration file specified.
    >>> apollo = ApolloPyClient()
    >>> apollo.connect("http://192.168.1.111:6600", "table_use")
    >>> apollo.get("news_topic_ch")
    'news_topic_ch_1'

    # Specify configuration file,
    # Cache configuration information when there is no network.
    >>> apollo = ApolloPyClient("apollo.json")
    >>> apollo.connect("http://192.168.1.111:6600", "table_use")
    >>> apollo.get("news_topic_ch")
    'news_topic_ch_1'

    # Turn on scheduled updates and hot update configurations,
    # The program will open two threads background polling update,
    # Remember to close the background thread.
    >>> apollo = ApolloPyClient("apollo.json")
    >>> apollo.connect("http://192.168.1.111:6600", "table_use")
    >>> apollo.listen()
    >>> apollo.get("news_topic_ch")
    'news_topic_ch_1'
    >>> apollo.close()

    # Specify listen schedule update sleep time interval.
    >>> apollo = ApolloPyClient("apollo.json")
    >>> apollo.connect("http://192.168.1.111:6600", "table_use")
    >>> apollo.listen(interval=60)
    >>> apollo.get("news_topic_ch")
    'news_topic_ch_1'
    >>> apollo.close()

    """
    def __init__(self, filename=None):
        """
        Initialize configuration information if a configuration file is specified.
        Args:
            filename (str): configuration file, default None.
        """
        self.sign = True
        self.releaseKey = None
        self.filename = None
        self.namespaceName = None
        self.notificationId = -1
        self.configurations = {}
        self.filename = filename
        if filename:
            if os.path.exists(filename):
                with open(filename, "r", encoding="utf-8") as f:
                    r = f.read()
                try:
                    self.configurations = json.loads(r)
                    self.releaseKey = self.configurations["releaseKey"]
                except Exception as e:
                    logging.warning("Missing profile information")

    def get(self, key):
        """
        Get the configuration value by key.
        """
        return self.configurations.get(key)

    def connect(self, server_url, appId, clusterName="default", namespaceName="application"):
        """
        Connect to the Configuration Center service for the latest configuration.
        Args:
            server_url (str): Apollo configuration service address.
            appId (str): Application's appId.
            clusterName (str): Cluster name, default default.
            namespaceName (str): Namespace's name, default application.
        """
        self.namespaceName = namespaceName
        self.config_url = "{server_url}/configs/{appId}/{clusterName}/{namespaceName}".format(
            server_url = server_url.strip("/"), 
            appId = appId, 
            clusterName = clusterName, 
            namespaceName = namespaceName
        )
        self.notification_url = "{}/notifications/v2?appId={}&cluster={}&notifications=".format(
            server_url.strip("/"), 
            appId, 
            clusterName
        )
        try:
            self.update_config()
        except Exception as e:
            logging.error("Failed to establish a new connection: {}".format(server_url))

    def update_config(self):
        """
        Update local configuration if configuration is updated
        Judging by releaseKey
        """
        url = self.config_url
        if self.releaseKey:
            url += "?releaseKey={}".format(self.releaseKey)
        r = requests.get(url)
        if not r.status_code == 304:
            self.configurations = r.json()['configurations']
            self.releaseKey = r.json()["releaseKey"]
            self.configurations["releaseKey"] = self.releaseKey
            if self.filename:
                with open(self.filename, "w", encoding="utf-8") as f:
                    f.write(json.dumps(self.configurations))
        logging.info("update config done. requests.status_code: {}".format(requests.status_code))

    def schedule_update(self, interval):
        """
        Schedule update configuration, time.sleep(interval)
        Args:
            interval (int): time interval
        """
        while self.sign:
            try:
                self.update_config()
            except Exception as e:
                logging.warning(e)
            time.sleep(interval)

    def perceptual_update(self, timeout):
        """
        Perceptual update configuration
        """
        while self.sign:
            try:
                url = self.notification_url+quote('[{"namespaceName": "%s", "notificationId": %d}]'%(self.namespaceName, self.notificationId))
                r = requests.get(url)
                if not r.status_code == 304:
                    self.notificationId = r.json()[0]["notificationId"]
                    self.update_config()
                    logging.info("perceptual update done.")
            except Exception as e:
                logging.warning(e)
                time.sleep(60)

    def listen(self, interval=30, timeout=70):
        """
        Turn on two threaded regular update and perceptual update.
        Don't laugh at me, this step is simple to implement.
        Args:
            interval (int): schedule update sleep time interval, default 30.
            timeout (int): perceptual update request timeout, default 70, must be > 60.
        """
        _start_new_thread(self.schedule_update, (interval,))
        _start_new_thread(self.perceptual_update, (timeout,))

    def close(self):
        """
        Turn off scheduled updates and aware update threads.
        """
        self.sign = False
        logging.info("Stopping listener...")

if __name__ == "__main__":
    doctest.testmod(verbose=2)
