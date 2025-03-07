#!/usr/bin/python
# -*- coding: utf-8 -*-

from abc import abstractmethod


class Connector:

    @abstractmethod
    def get_df(self, *args, **kwargs):
        pass

    @abstractmethod
    def upload_df(self, *args, **kwargs):
        pass