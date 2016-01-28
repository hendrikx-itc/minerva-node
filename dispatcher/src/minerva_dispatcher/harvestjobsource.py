# -*- coding: utf-8 -*-
"""Provides the JobSource class."""
import os

from minerva.system.jobsource import JobSource
from minerva.system.job import JobDescriptor

JOB_TYPE = "harvest"


class HarvestJobSource(JobSource):
    def job_description(self, dir_path):
        description = {"uri": dir_path}
        description.update(self.config["job_config"])
        return description

    def create_job(self, file_path):
        return JobDescriptor(
            job_type=JOB_TYPE,
            description=self.job_description(file_path),
            size=get_file_size(file_path),
            job_source_id=self.id
        )


def get_file_size(file_path):
    try:
        return os.path.getsize(file_path)
    except OSError as exc:
        raise Exception("could not get size of file: {}".format(exc))
