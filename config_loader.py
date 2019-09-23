#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Sep 22 15:56:53 2019

@author: nithin
"""

import configparser
import os


class Config:
    """Interact with configuration variables."""

    configParser = configparser.ConfigParser()
    configFilePath = (os.path.join(os.getcwd(), 'config.ini'))

    @classmethod
    def __init__(cls):
        """Start config by reading config.ini."""
        cls.configParser.read(cls.configFilePath)
        
    @classmethod
    def common(cls, key):
        """Get global values from config.ini."""
        return cls.configParser.get('COMMON', key)

    @classmethod
    def prod(cls, key):
        """Get prod values from config.ini."""
        return cls.configParser.get('PROD', key)

    @classmethod
    def dev(cls, key):
        """Get dev values from config.ini."""
        return cls.configParser.get('DEV', key)
    
Config()