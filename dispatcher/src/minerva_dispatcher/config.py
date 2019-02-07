# -*- coding: utf-8 -*-
"""Contains the configuration data for the dispatcher"""

job_source_data = [
    {
       "id": 1,
       "name": "aireas",
       "job_type": "harvest",
       "config": """
{
    "uri": "/input",
    "recursive": true,
    "match_pattern": ".*",
    "job_config": {
        "data_type": "aireas",
        "data_source": "aireas",
        "on_failure": {},
        "parser_config": {}
    }
}
"""
    }
]

rabbitmq_data = {
    "url": "amqp://guest:guest@rabbit:5672/%2F?connection_attempts=3&heartbeat_interval=3600",
    "routing_key": "minerva",
    "logger": "minerva-dispatcher",
    "sleeptime": 40
}
