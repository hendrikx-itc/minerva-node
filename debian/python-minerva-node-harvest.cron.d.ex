#
# Regular cron jobs for the python-minerva-node-harvest package
#
0 4	* * *	root	[ -x /usr/bin/python-minerva-node-harvest_maintenance ] && /usr/bin/python-minerva-node-harvest_maintenance
