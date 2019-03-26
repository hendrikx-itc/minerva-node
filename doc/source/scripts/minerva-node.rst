minerva-node
============

Synopsis
--------

    minerva-node [-c path] [--list-plugins] [--generate-configfile] [--slot slot-number]

Description
-----------

minerva-node is the process that reads jobs from the Minerva database queue,
chooses a matching plugin based on the job type and executes the job.

Options
-------

.. cmdoption:: -c <path>, --configfile <path>

   Specify which config file to use. By default minerva-node will use ``/etc/minerva/node.conf``.

.. cmdoption:: --generate-configfile

   Generate a template config file and send it to stdout.

.. cmdoption:: --list-plugins

   List all installed Node plugins.

.. cmdoption:: --slot <slot-number>

   Specify in which slot the Node should start. It affects the name of the PID
   file and log file. This is useful when running multiple Node instances on
   one server.


Generic options
---------------

.. cmdoption:: -h, --help

   Print a short description of all command line options and exit.

.. cmdoption:: -v, --version

   Print the version of minerva-node and exit.
