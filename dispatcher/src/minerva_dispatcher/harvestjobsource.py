# -*- coding: utf-8 -*-
"""
Provides the JobSource class.
"""
__docformat__ = "restructuredtext en"
__copyright__ = """
Copyright (C) 2008-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from minerva.system.jobsource import JobSource


class HarvestJobSource(JobSource):
    def job_description(self, path):
        description = {"uri": path}
        description.update(self.config["job_config"])
        return description
