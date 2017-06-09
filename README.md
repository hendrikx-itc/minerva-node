# Minerva ETL

A set of tools for performing ETL tasks on the Minerva database platform.

The main tools are:

- Dispatcher - For picking up jobs (files) and placing them on the Minerva job queue.
- Node - A generic worker process for picking up jobs from the Minerva job queue and processing them.
- Node plugins - Different types of processing logic for different types of jobs.
