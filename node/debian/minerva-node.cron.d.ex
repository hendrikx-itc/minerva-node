#
# Regular cron jobs for the minerva-node package
#
0 4	* * *	root	[ -x /usr/bin/minerva-node_maintenance ] && /usr/bin/minerva-node_maintenance
