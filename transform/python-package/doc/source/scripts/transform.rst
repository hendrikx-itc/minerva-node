transform
=========

Synopsis
--------

transform [-vhl] [--version] [-c path] [--generate-configfile] [-t timestamp]
    [-f function_set] [-g granularity] [--dest-granularity granularity]
    [--dest-entitytype entitytype] [--debug]

Description
-----------

transform runs one or more transformations.

Options
-------

.. cmdoption:: -c <path>, --configfile <path>

   Specify which config file to use. By default transform will use ``/etc/minerva/transform.conf``.

.. cmdoption:: --generate-configfile

   Generate a template config file and send it to stdout.

.. cmdoption:: -l, --list

   List available transformations (function sets), optionally filtered using the
   options :option:`-f`, :option:`-g`, :option:`--dest-granularity` and
   :option:`--dest-entitytype`.

.. cmdoption:: -t, --timestamp <timestamp>

   The destination timestamp of the transformation(s) with format
   ``yyyy-mm-ddThh:mm:ss+hh:ss``.

.. cmdoption:: -f, --function-set <id>|<name>

   Filter function sets by name or specify one by it's Id. If a name is specified
   it could be that multiple functions sets match because it is not necessarily
   unique. Further filtering can be done using the options :option:`-g`,
   :option:`--dest-granularity` and :option:`--dest-entitytype`. Use the
   :option:`-l` option to check if the filter is accurate.

.. cmdoption:: -g, --granularity <seconds>

   Filter function sets by the granularity of the required source data.

.. cmdoption:: --dest-granularity <seconds>

   Filter function sets by the granularity of the resulting data.

.. cmdoption:: --dest-entitytype <name>

   Filter function sets by the name of the entity type of the resulting data.

.. cmdoption:: --start <timestamp>

   The start of a range of timestamps to process. This option cannot be used
   together with the `--timestamp` option.

.. cmdoption:: --end <timestamp>

   The end of a range of timestamps to process. This option must be used together
   whith the `--start` option and cannot be used together with the `--timestamp`
   option.

Generic Options
---------------

.. cmdoption:: -h, --help

   Print a short description of all command line options and exit.

.. cmdoption:: --version

   Print the version of transform and exit.

Examples
--------

Run transformation for function set with Id 211 and timestamp '2013-01-28 13:00':

    ``transform -f 211 -t "2013-01-28T13:00:00+01:00"``

Run transformations for all function sets from '2013-01-01 00:00' until now:

    ``transform --start "2013-01-01T00:00:00+01:00"``
