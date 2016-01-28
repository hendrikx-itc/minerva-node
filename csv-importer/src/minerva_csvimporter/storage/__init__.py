# -*- coding: utf-8 -*-
from minerva_csvimporter.storage.storage import Storage
from minerva_csvimporter.storage.trendstorage import TrendStorage
from minerva_csvimporter.storage.attributestorage import AttributeStorage
from minerva_csvimporter.storage.notificationstorage import NotificationStorage

type_map = {
    "trend": TrendStorage,
    "attribute": AttributeStorage,
    "notification": NotificationStorage
}