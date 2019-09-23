#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Sep 21 20:44:53 2019

@author: nithin
"""

from config_loader import Config

print(Config.prod('DATABASE'))
