exec-node-job
=============

Synopsis
--------

    exec-node-job [-c path] [--list-plugins] [--generate-configfile]

Description
-----------

exec-node-job can be used to run a job without having to create a record for it
in the Minerva database. This can be useful for testing plugins.

Options
-------

.. cmdoption:: -c <path>, --configfile <path>

   Specify which config file to use. By default exec-node-job will use ``/etc/minerva/node.conf``.

.. cmdoption:: --generate-configfile

   Generate a template config file and send it to stdout.

.. cmdoption:: --list-plugins

   List all installed Node plugins.


Generic options
---------------

.. cmdoption:: -h, --help

   Print a short description of all command line options and exit.

.. cmdoption:: -v, --version

   Print the version of exec-node-job and exit.
