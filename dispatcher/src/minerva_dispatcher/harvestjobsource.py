# -*- coding: utf-8 -*-
import os
import json


from minerva_dispatcher.jobsource import JobSource


class HarvestJobSource(JobSource):
    def job_description(self, file_path):
        description = {}
        description.update(self.config)
        description['uri'] = file_path
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
