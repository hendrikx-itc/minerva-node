#
# Regular cron jobs for the minerva-dispatcher package
#
0 4	* * *	root	[ -x /usr/bin/minerva-dispatcher_maintenance ] && /usr/bin/minerva-dispatcher_maintenance
