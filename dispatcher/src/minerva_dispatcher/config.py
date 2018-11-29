# -*- coding: utf-8 -*-
"""Contains the configuration data for the dispatcher"""

job_source_data = [
    {
       "id": 1,
       "name": "oss-rc-3g-pm",
       "job_type": "harvest",
       "config": """
{
    "uri": "/data/Udex/oss-rc/3G/pm",
    "recursive": true,
    "match_pattern": "[^\\\\.]",
    "job_config": {
        "datatype": "pm_3gpp",
        "datasource": "oss-rc-3g-pm",
        "on_failure": {"name": "move", "args": ["/data/fringe/oss-rc/3g/pm"]},          "parser_config": {}
    }
}
"""
    }
]

rabbitmq_data = {
    "url": "amqp://guest:guest@localhost:5672/%2F?connection_attempts=3&heartbeat_interval=3600",
    "queue": None, # "queue=None implies queue=JOB_TYPE
    "routing_key": "minerva",
    "logger": "minerva-dispatcher"
}
