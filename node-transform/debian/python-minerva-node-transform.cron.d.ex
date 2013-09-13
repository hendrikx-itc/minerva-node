#
# Regular cron jobs for the python-minerva-node-transform package
#
0 4	* * *	root	[ -x /usr/bin/python-minerva-node-transform_maintenance ] && /usr/bin/python-minerva-node-transform_maintenance
