# -*- coding: utf-8 -*-
"""Provides the JobSource class."""
import os
import json

from minerva.system.jobsource import JobSource


class HarvestJobSource(JobSource):
    def __init__(self, id, name, job_type, config, publisher):
        JobSource.__init__(self, id, name, job_type, config)
        self.queue = publisher

    def job_description(self, dir_path):
        description = {"uri": dir_path}
        description.update(self.config["job_config"])
        return description

    def create_job(self, file_path):
        return json.dumps({
            'job_type': self.job_type,
            'description': self.job_description(file_path),
            'size': get_file_size(file_path),
        })


def get_file_size(file_path):
    try:
        return os.path.getsize(file_path)
    except OSError as exc:
        raise Exception("could not get size of file: {}".format(exc))
