# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2014 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from minerva_csvimporter.storage.storage import Storage
from minerva_csvimporter.storage.trendstorage import TrendStorage
from minerva_csvimporter.storage.attributestorage import AttributeStorage
from minerva_csvimporter.storage.notificationstorage import NotificationStorage

type_map = {
    "trend": TrendStorage,
    "attribute": AttributeStorage,
    "notification": NotificationStorage
}