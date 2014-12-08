Materialize Minerva Extension
=============================

The Materialize extension for Minerva provides a specialized implementation of
materialization for the standard trend storage class.


Dependency calculation
----------------------



SELECT ((foo.dependencies).trendstore)::text, (foo.dependencies).level
FROM (SELECT materialization.dependencies('trf-availability_Cell_wk')) foo
JOIN materialization.type ON type.src_trendstore_id = ((foo.dependencies).trendstore).id                      ORDER BY (foo.dependencies).level DESC;


Prioritization
==============

It is important to execute materializations in the right order, so that the
user has the most urgent data first. What the most urgent data is, depends on
the user (customer).

We have to maximize the use of our resources.


R - resources
L - list of materializations to do
C - the cost for a specific materialization


G - materialization goup (tag)

tag_a     tag_b       tag_c
