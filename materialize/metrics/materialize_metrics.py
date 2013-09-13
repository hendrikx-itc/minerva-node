"""
Creates metrics for state_queue view
"""
from contextlib import closing
from minerva.util import first


METRICS_ENTITYTYPE_NAME = "MaterializeQueue"
METRICS_ENTITY_DN_PART = "{}=Standard".format(METRICS_ENTITYTYPE_NAME)


def get_metrics(conn, granularity, timestamp):
    metrics = [
        ("runnable", "count(*)"),
        ("runnable_heavy", "count(CASE WHEN tag = 'heavy' THEN 1 ELSE null END)"),
        ("runnable_medium", "count(CASE WHEN tag = 'medium' THEN 1 ELSE null END)"),
        ("runnable_light", "count(CASE WHEN tag = 'light' THEN 1 ELSE null END)")]

    query = "SELECT {} FROM materialization.tagged_runnable_materializations".format(
        ",".join(metric[1] for metric in metrics))

    trend_names = map(first, metrics)

    with closing(conn.cursor()) as cursor:
        cursor.execute(query)
        trend_values = cursor.fetchone()

    return (trend_names, trend_values)

