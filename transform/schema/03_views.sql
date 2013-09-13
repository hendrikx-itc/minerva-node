SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;

SET search_path = transform, pg_catalog;

-- View 'runnable_jobs'

CREATE VIEW tagged_runnable_jobs AS
	SELECT state.function_set_id, dest_timestamp, processed_max_modified, t.name as tag
		FROM transform.state
		JOIN transform.function_set_tag_link fstl ON fstl.function_set_id = state.function_set_id
		JOIN directory.tag t ON t.id = fstl.tag_id
		JOIN transform.function_set fs ON fs.id = state.function_set_id
		WHERE
			(max_modified > processed_max_modified OR processed_max_modified IS NULL)
			AND job_id IS NULL
			AND transform.runnable(fs.dest_granularity, dest_timestamp, max_modified)
		ORDER BY fs.dest_granularity ASC, dest_timestamp DESC;

ALTER VIEW tagged_runnable_jobs OWNER TO minerva_admin;

GRANT ALL ON transform.tagged_runnable_jobs TO minerva_admin;
GRANT SELECT ON transform.tagged_runnable_jobs TO minerva;
GRANT INSERT,DELETE,UPDATE ON transform.tagged_runnable_jobs TO minerva_writer;

-- View 're_runnable_jobs'

CREATE VIEW tagged_re_runnable_jobs AS
SELECT state.function_set_id, dest_timestamp, processed_max_modified, t.name as tag
	FROM transform.state
	JOIN transform.function_set_tag_link fstl ON fstl.function_set_id = state.function_set_id
	JOIN directory.tag t ON t.id = fstl.tag_id
	JOIN transform.function_set fs ON fs.id = state.function_set_id
	JOIN system.job j ON j.id = job_id
	WHERE
		(max_modified > processed_max_modified OR processed_max_modified IS NULL)
		AND NOT j.finished IS NULL
		AND transform.runnable(fs.dest_granularity, dest_timestamp, max_modified)
	ORDER BY fs.dest_granularity ASC, dest_timestamp DESC;

ALTER VIEW tagged_re_runnable_jobs OWNER TO minerva_admin;

GRANT ALL ON transform.tagged_re_runnable_jobs TO minerva_admin;
GRANT SELECT ON transform.tagged_re_runnable_jobs TO minerva;
GRANT INSERT,DELETE,UPDATE ON transform.tagged_re_runnable_jobs TO minerva_writer;


CREATE VIEW state_queue AS
SELECT
	f.id,
	f.enabled,
	tag.name tag,
	transform.runnable(f.dest_granularity, s.dest_timestamp, s.max_modified),
	f.name,
	f.source_granularity,
	f.dest_granularity,
	s.dest_timestamp,
	s.processed_max_modified,
	s.max_modified,
	s.processed_max_modified - s.max_modified as interval,
	j.id job_id,
	j.state,
	j.started,
	j.finished
FROM transform.state s
LEFT JOIN system.job j on j.id = s.job_id
JOIN transform.function_set f on f.id = s.function_set_id
LEFT JOIN transform.function_set_tag_link fstl on fstl.function_set_id = f.id
LEFT JOIN directory.tag on tag.id = fstl.tag_id
WHERE
(
	s.processed_max_modified < s.max_modified OR
	s.processed_max_modified IS NULL
)
AND s.dest_timestamp < now()
ORDER BY s.dest_timestamp asc, f.dest_granularity, f.id;

ALTER VIEW state_queue OWNER TO minerva_admin;

GRANT ALL ON transform.state_queue TO minerva_admin;
GRANT SELECT ON transform.state_queue TO minerva;
GRANT INSERT,DELETE,UPDATE ON transform.state_queue TO minerva_writer;


CREATE VIEW transformables AS
	SELECT
		fs.id AS function_set_id,
		trend.get_timestamp_for(fs.dest_granularity, timestamp) AS timestamp,
		max(m."end")
	FROM trend.modified m
	JOIN trend.partition p ON
					m.table_name = p.table_name
	JOIN trend.trendstore ts ON ts.id = p.trendstore_id
	JOIN transform.function_set fs ON
					ARRAY[ts.datasource_id] <@ fs.source_datasource_ids AND
					ts.entitytype_id = fs.source_entitytype_id AND
					ts.granularity = fs.source_granularity
	GROUP BY fs.id, trend.get_timestamp_for(fs.dest_granularity, timestamp);

ALTER VIEW transformables OWNER TO minerva_admin;

GRANT ALL ON transform.transformables TO minerva_admin;
GRANT SELECT ON transform.transformables TO minerva;
GRANT INSERT,DELETE,UPDATE ON transform.transformables TO minerva_writer;
