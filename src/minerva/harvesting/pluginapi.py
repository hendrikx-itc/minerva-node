# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""

class HarvestPlugin(object):
	def api_versions(self):
		"""
		Sub classes should return a list with supported API versions
		"""
		raise NotImplemented()

	def parser(self):
		"""
		Version 4
		"""
		raise NotImplemented()

	def create_parser(self):
		"""
		Version 4
		"""
		raise NotImplemented()
