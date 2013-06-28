#
# Regular cron jobs for the python-minerva-node-dummy package
#
0 4	* * *	root	[ -x /usr/bin/python-minerva-node-dummy_maintenance ] && /usr/bin/python-minerva-node-dummy_maintenance
