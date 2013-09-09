enqueue-node-job
================

.. program:: enqueue-node-job

Synopsis
--------

    enqueue-node-job [-c path] [--generate-configfile] job

Description
-----------

enqueue-node-job is the command line utility for adding jobs to the Minerva
database queue. This can be useful for testing purposes or small Minerva
installations.

Example
-------

For a simple test using the dummy job type, the following command can be used::

   enqueue-node-job '["dummy", "{}", 0, 1]'


Options
-------

.. cmdoption:: -c <path>, --configfile <path>

   Specify which config file to use. By default enqueue-node-job will use ``/etc/minerva/enqueue-node-job.conf``.

.. cmdoption:: --generate-configfile

   Generate a template config file and send it to stdout.

.. describe:: <job>

   The specification of the job to enqueue. This can be the file name of a file
   containing a JSON structure or the JSON structure itself. The JSON structure
   has the following format::

       [<job_type>, <description>, <size>, <job_source_id>]


Generic options
---------------

.. cmdoption:: -h, --help

   Print a short description of all command line options and exit.

.. cmdoption:: -v, --version

   Print the version of minerva-node and exit.
