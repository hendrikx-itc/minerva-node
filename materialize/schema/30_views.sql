SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;

SET search_path = materialization, pg_catalog;


-- View 'tagged_runnable_materializations'

CREATE OR REPLACE VIEW tagged_runnable_materializations AS
	SELECT state.type_id, timestamp, processed_max_modified, t.name as tag
		FROM materialization.state
		JOIN materialization.type_tag_link mtl ON mtl.type_id = state.type_id
		JOIN directory.tag t ON t.id = mtl.tag_id
		JOIN materialization.type mt ON mt.id = state.type_id
		JOIN trend.trendstore ts ON ts.id = mt.dst_trendstore_id
		LEFT JOIN system.job j ON j.id = state.job_id
		WHERE
			(max_modified > processed_max_modified OR processed_max_modified IS NULL)
			AND (state.job_id IS NULL OR NOT j.state IN ('queued', 'running'))
			AND materialization.runnable(mt, timestamp, max_modified)
		ORDER BY ts.granularity ASC, timestamp DESC;

ALTER VIEW tagged_runnable_materializations OWNER TO minerva_admin;

GRANT ALL ON materialization.tagged_runnable_materializations TO minerva_admin;
GRANT SELECT ON materialization.tagged_runnable_materializations TO minerva;
GRANT INSERT,DELETE,UPDATE ON materialization.tagged_runnable_materializations TO minerva_writer;


-- View 'materializables'

CREATE VIEW materializables AS
	SELECT
		mt.id AS type_id,
		trend.get_timestamp_for(dst.granularity, mdf.timestamp) AS timestamp,
		max(mdf."end") AS max_modified
	FROM trend.modified mdf
	JOIN trend.partition p ON
		mdf.table_name = p.table_name
	JOIN trend.view_trendstore_link vtl ON
		vtl.trendstore_id = p.trendstore_id
	JOIN trend.view v ON v.id = vtl.view_id
	JOIN materialization.type mt ON
		mt.src_trendstore_id = v.trendstore_id
	JOIN trend.trendstore dst ON
		dst.id = mt.dst_trendstore_id
	GROUP BY mt.id, trend.get_timestamp_for(dst.granularity, mdf.timestamp);

ALTER VIEW materializables OWNER TO minerva_admin;

GRANT ALL ON materialization.materializables TO minerva_admin;

GRANT ALL ON materialization.materializables TO minerva_admin;
GRANT SELECT ON materialization.materializables TO minerva;
GRANT INSERT,DELETE,UPDATE ON materialization.materializables TO minerva_writer;



CREATE VIEW trend_ext AS
SELECT
	t.id,
	t.name,
	ds.name AS datasource_name,
	et.name AS entitytype_name,
	ts.granularity,
	CASE
		WHEN m.src_trendstore_id IS NULL THEN false
		ELSE true
	END AS materialized
	FROM trend.trend t
	JOIN trend.trendstore_trend_link ttl ON ttl.trend_id = t.id
	JOIN trend.trendstore ts ON ts.id = ttl.trendstore_id
	JOIN directory.datasource ds ON ds.id = ts.datasource_id
	JOIN directory.entitytype et ON et.id = ts.entitytype_id
	LEFT JOIN materialization.type m ON m.src_trendstore_id = ts.id;


ALTER VIEW trend_ext OWNER TO minerva_admin;

GRANT SELECT ON TABLE trend_ext TO minerva;

