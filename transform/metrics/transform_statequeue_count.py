"""
Creates metrics for state_queue view 
"""
from contextlib import closing
from minerva.util import first


METRICS_ENTITYTYPE_NAME = "StateQueue"
METRICS_ENTITY_DN_PART = "{}=Standard".format(METRICS_ENTITYTYPE_NAME)


def get_metrics(conn, granularity, timestamp):
	metrics = [
			("total", "COUNT(*)"),
			("runnable", "COUNT(CASE WHEN runnable = True THEN 1 ELSE null END)" ),
			("runnable_enabled", "COUNT(CASE WHEN (runnable = True AND enabled = True) THEN 1 ELSE null END)" ),
			("notrunnable", "COUNT(CASE WHEN runnable = False THEN 1 ELSE null END)" )]

	query = "SELECT {} FROM transform.state_queue ".format(
		",".join(metric[1] for metric in metrics))

	trend_names = map(first, metrics)

	with closing(conn.cursor()) as cursor:
		cursor.execute(query)
		trend_values = cursor.fetchone()
	
	return (trend_names, trend_values)

