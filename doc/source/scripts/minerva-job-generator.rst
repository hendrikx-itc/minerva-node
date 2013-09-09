minerva-job-generator
=====================

Synopsis
--------

    minerva-job-generator [-c path] [--generate-configfile]

Description
-----------

minerva-job-generator creates dummy jobs and adds them to the Minerva database
job queue for testing a Minerva Node.

Options
-------

.. cmdoption:: -c <path>, --configfile <path>

   Specify which config file to use. By default minerva-job-generator will use ``/etc/minerva/job-generator.conf``.

.. cmdoption:: --generate-configfile

   Generate a template config file and send it to stdout.


Generic options
---------------

.. cmdoption:: -h, --help

   Print a short description of all command line options and exit.

.. cmdoption:: -v, --version

   Print the version of minerva-job-generator and exit.
