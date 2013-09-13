SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;

SET search_path = transform, pg_catalog;


CREATE OR REPLACE FUNCTION identity(integer)
	RETURNS integer
AS $$
	SELECT $1;
$$ LANGUAGE SQL IMMUTABLE;

ALTER FUNCTION identity(integer)
	OWNER TO postgres;


CREATE OR REPLACE FUNCTION safe_division(numerator smallint, denominator smallint)
	RETURNS double precision
AS $$
SELECT CASE
	WHEN $2 = 0 THEN
		NULL
	ELSE
		CAST($1 AS float) / $2
	END;
$$ LANGUAGE SQL IMMUTABLE;

ALTER FUNCTION safe_division(smallint, smallint)
	OWNER TO postgres;


CREATE OR REPLACE FUNCTION safe_division(numerator integer, denominator integer)
	RETURNS double precision
AS $$
SELECT CASE
	WHEN $2 = 0 THEN
		NULL
	ELSE
		CAST($1 AS float) / $2
	END;
$$ LANGUAGE SQL IMMUTABLE;

ALTER FUNCTION safe_division(integer, integer)
	OWNER TO postgres;


CREATE OR REPLACE FUNCTION safe_division(numerator real, denominator real)
	RETURNS real
AS $$
SELECT CASE
	WHEN $2 = 0 THEN
		NULL
	ELSE
		$1 / $2
	END;
$$ LANGUAGE SQL IMMUTABLE;

ALTER FUNCTION safe_division(real, real)
	OWNER TO postgres;

CREATE OR REPLACE FUNCTION safe_division(numerator double precision, denominator double precision)
	RETURNS double precision
AS $$
SELECT CASE
	WHEN $2 = 0 THEN
		NULL
	ELSE
		$1 / $2
	END;
$$ LANGUAGE SQL IMMUTABLE;

ALTER FUNCTION safe_division(double precision, double precision)
	OWNER TO postgres;

CREATE OR REPLACE FUNCTION multiply(integer, integer)
	RETURNS integer
AS $$
	SELECT $1 * $2;
$$ LANGUAGE SQL IMMUTABLE;

ALTER FUNCTION transform.multiply(integer, integer)
	OWNER TO postgres;


CREATE OR REPLACE FUNCTION subtract(integer, integer)
	RETURNS integer
AS $$
	SELECT $1 - $2;
$$ LANGUAGE SQL IMMUTABLE;

ALTER FUNCTION transform.subtract(integer, integer)
	OWNER TO postgres;


CREATE OR REPLACE FUNCTION add(VARIADIC arr numeric[])
	RETURNS numeric
AS $$
	SELECT sum($1[i]) FROM generate_subscripts($1,1) g(i);
$$ LANGUAGE SQL IMMUTABLE;

ALTER FUNCTION transform.add(VARIADIC arr numeric[])
	OWNER TO postgres;


CREATE OR REPLACE FUNCTION add_ext(VARIADIC arr numeric[])
	RETURNS numeric
AS $$
	SELECT sum(coalesce($1[i],0)) FROM generate_subscripts($1,1) g(i);
$$ LANGUAGE SQL IMMUTABLE;

ALTER FUNCTION transform.add_ext(VARIADIC arr numeric[])
	OWNER TO postgres;


CREATE OR REPLACE FUNCTION get_sources(function_set_id integer)
	RETURNS trend.trendstore
AS $$
	SELECT ts
	FROM transform.function_set fs
	JOIN trend.trendstore ts ON
		ARRAY[ts.datasource_id] <@ source_datasource_ids AND
		ts.entitytype_id = source_entitytype_id AND
		ts.granularity = source_granularity
	WHERE fs.id = $1;
$$ LANGUAGE SQL STABLE;


CREATE OR REPLACE FUNCTION update_state()
	RETURNS void
AS $$
	INSERT INTO transform.state(function_set_id, dest_timestamp, processed_max_modified, max_modified)
		SELECT fs.id, trend.get_timestamp_for(fs.dest_granularity, timestamp) AS dest_timestamp, NULL, max(m."end")
		FROM trend.modified m
		JOIN trend.partition p ON
			m.table_name = p.table_name
		JOIN trend.trendstore ts ON ts.id = p.trendstore_id
		JOIN transform.function_set fs ON
			ARRAY[ts.datasource_id] <@ fs.source_datasource_ids AND
			ts.entitytype_id = fs.source_entitytype_id AND
			ts.granularity = fs.source_granularity AND
			fs.enabled = TRUE
		LEFT JOIN transform.state ON
			fs.id = state.function_set_id AND
			transform.state.dest_timestamp = trend.get_timestamp_for(fs.dest_granularity, timestamp)
		WHERE state.function_set_id IS NULL
		GROUP BY fs.id, trend.get_timestamp_for(fs.dest_granularity, timestamp);

	INSERT INTO transform.state(function_set_id, dest_timestamp, processed_max_modified, max_modified)
		SELECT fs.id, trend.get_timestamp_for(fs.dest_granularity, timestamp) AS dest_timestamp, NULL, max(m."end")
		FROM trend.modified m
		JOIN trend.partition p ON
			m.table_name = p.table_name
		JOIN trend.view_trendstore_link vtl ON
			vtl.trendstore_id = p.trendstore_id
		JOIN trend.view v ON
			v.id = vtl.view_id
		JOIN trend.trendstore ts ON
			ts.id = v.trendstore_id
		JOIN transform.function_set fs ON
			ARRAY[ts.datasource_id] <@ fs.source_datasource_ids AND
			ts.entitytype_id = fs.source_entitytype_id AND
			ts.granularity = fs.source_granularity AND
			fs.enabled = TRUE
		LEFT JOIN transform.state ON
			fs.id = state.function_set_id AND
			transform.state.dest_timestamp = trend.get_timestamp_for(fs.dest_granularity, timestamp)
		WHERE state.function_set_id IS NULL
		GROUP BY fs.id, trend.get_timestamp_for(fs.dest_granularity, timestamp);

	WITH updates AS (
		SELECT fs.id AS function_set_id, trend.get_timestamp_for(fs.dest_granularity, timestamp) AS dest_timestamp, NULL, max(m."end") AS max_modified
		FROM trend.modified m
		JOIN trend.partition p ON
			m.table_name = p.table_name
		JOIN trend.trendstore ts ON
			ts.id = p.trendstore_id
		JOIN transform.function_set fs ON
			ARRAY[ts.datasource_id] <@ fs.source_datasource_ids AND
			ts.entitytype_id = fs.source_entitytype_id AND
			ts.granularity = fs.source_granularity AND
			fs.enabled = TRUE
		JOIN transform.state ON
			fs.id = state.function_set_id AND
			trend.get_timestamp_for(fs.dest_granularity, timestamp) = state.dest_timestamp
		WHERE m."end" > state.max_modified OR state.max_modified IS NULL
		GROUP BY fs.id, trend.get_timestamp_for(fs.dest_granularity, timestamp)
	)
	UPDATE transform.state
		SET max_modified = updates.max_modified
	FROM updates
	WHERE
		state.function_set_id = updates.function_set_id AND
		state.dest_timestamp = updates.dest_timestamp;

	WITH obsolete AS (
		SELECT state.function_set_id, state.dest_timestamp
		FROM transform.state
		LEFT JOIN transform.transformables tfs ON
			tfs.function_set_id = state.function_set_id AND
			tfs.timestamp = state.dest_timestamp
		WHERE tfs.function_set_id IS null
	)
	DELETE FROM transform.state
	USING obsolete
	WHERE state.function_set_id = obsolete.function_set_id AND state.dest_timestamp = obsolete.dest_timestamp;
$$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION runnable(dest_granularity varchar, dest_timestamp timestamp with time zone, max_modified timestamp with time zone)
	RETURNS boolean
AS $$
BEGIN
	IF dest_granularity = '1800' or dest_granularity = '900' or dest_granularity = '300' THEN
		RETURN dest_timestamp < now() AND max_modified < now() - interval '180 seconds';
	ELSIF dest_granularity = '3600' THEN
		RETURN dest_timestamp < now() - interval '15 minutes' AND max_modified < now() - interval '5 minutes';
	ELSE
		RETURN dest_timestamp < now() - interval '3 hours' AND max_modified < now() - interval '15 minutes';
	END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE;


CREATE OR REPLACE FUNCTION render_job_json(function_set_id integer, dest_timestamp timestamp with time zone)
	RETURNS character varying
AS $$
	SELECT format('{"function_set_id": %s, "dest_timestamp": "%s"}', $1, $2);
$$ LANGUAGE SQL IMMUTABLE;


CREATE OR REPLACE FUNCTION compile_transform_job(function_set_id integer, dest_timestamp timestamp with time zone)
	RETURNS integer
AS $$
DECLARE
	job_description text;
	new_job_id integer;
BEGIN
	job_description := transform.render_job_json(function_set_id, dest_timestamp);

	SELECT system.create_job('transform', job_description, 1, job_source.id) INTO new_job_id
		FROM system.job_source
		WHERE name = 'compile-transform-jobs';

	UPDATE transform.state
		SET job_id = new_job_id
		WHERE state.function_set_id = $1 AND state.dest_timestamp = $2;

	RETURN new_job_id;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE OR REPLACE FUNCTION "compile-rerun-transform-jobs-limited"(tag varchar, job_limit integer)
	RETURNS integer
AS $$
DECLARE
	created_jobs integer := 0;
	new_job_count integer;
	remote_query text;
	replicated_server_conn system.setting;
BEGIN
	new_job_count = transform.open_job_slots(job_limit);

	replicated_server_conn = system.get_setting('replicated_server_conn');

	IF replicated_server_conn IS NULL THEN
		SELECT COUNT(transform.compile_transform_job(j.function_set_id, j.dest_timestamp)) INTO created_jobs
		FROM
		(
			SELECT function_set_id, dest_timestamp
			FROM transform.tagged_re_runnable_jobs trrj
			WHERE trrj.tag = $1
			LIMIT new_job_count
		) j;
	ELSE
		remote_query = format('SELECT function_set_id, dest_timestamp
			FROM transform.tagged_re_runnable_jobs
			WHERE tag = ''%s''
			LIMIT %s', tag, new_job_count);

		SELECT COUNT(transform.compile_transform_job(replicated_state.function_set_id, replicated_state.dest_timestamp)) INTO created_jobs
		FROM public.dblink(replicated_server_conn.value, remote_query)
			AS replicated_state(function_set_id integer, dest_timestamp timestamp with time zone)
		JOIN transform.tagged_re_runnable_jobs rj ON replicated_state.function_set_id = rj.function_set_id
			AND replicated_state.dest_timestamp = rj.dest_timestamp;
	END IF;

	RETURN created_jobs;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION open_job_slots(slot_count integer)
	RETURNS integer
AS $$
	SELECT greatest($1 - COUNT(*), 0)::integer FROM system.job WHERE type = 'transform' and (state = 'running' or state = 'queued');
$$ LANGUAGE SQL STABLE;


CREATE OR REPLACE FUNCTION "compile-transform-jobs-limited"(tag varchar, job_limit integer)
	RETURNS integer
AS $$
DECLARE
	created_jobs integer := 0;
	new_job_count integer;
	remote_query text;
	replicated_server_conn system.setting;
BEGIN
	new_job_count = transform.open_job_slots(job_limit);

	replicated_server_conn = system.get_setting('replicated_server_conn');

	IF replicated_server_conn IS NULL THEN
		SELECT COUNT(transform.compile_transform_job(j.function_set_id, j.dest_timestamp)) INTO created_jobs
		FROM
		(
			SELECT function_set_id, dest_timestamp
			FROM transform.tagged_runnable_jobs trj
			WHERE trj.tag = $1
			LIMIT new_job_count
		) j;
	ELSE
		remote_query = format('SELECT function_set_id, dest_timestamp
			FROM transform.tagged_runnable_jobs
			WHERE tag = ''%s''
			LIMIT %s', tag, new_job_count);

		SELECT COUNT(transform.compile_transform_job(replicated_state.function_set_id, replicated_state.dest_timestamp)) INTO created_jobs
		FROM public.dblink(replicated_server_conn.value, remote_query)
			AS replicated_state(function_set_id integer, dest_timestamp timestamp with time zone)
		JOIN transform.tagged_runnable_jobs rj ON replicated_state.function_set_id = rj.function_set_id
			AND replicated_state.dest_timestamp = rj.dest_timestamp;
	END IF;

	RETURN created_jobs;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION "compile-transform-jobs"()
	RETURNS integer
AS $$
DECLARE
	job_description text;
	new_job_id integer;
	created_jobs integer := 0;
	c CURSOR FOR SELECT state.function_set_id, dest_timestamp, processed_max_modified
		FROM transform.state
		JOIN transform.function_set_tag_link fstl ON fstl.function_set_id = state.function_set_id
		JOIN directory.tag t ON t.id = fstl.tag_id
		WHERE dest_timestamp < now()
			AND (max_modified > processed_max_modified OR processed_max_modified IS NULL)
			AND job_id IS NULL
			AND max_modified < now() - interval '60 seconds'
		FOR UPDATE;
BEGIN
	FOR r IN c LOOP
		SELECT transform.compile_transform_job(r.function_set_id, r.dest_timestamp);

		created_jobs := created_jobs + 1;
	END LOOP;

	RETURN created_jobs;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION "compile-rerun-transform-jobs"()
	RETURNS integer
AS $$
DECLARE
	job_description text;
	new_job_id integer;
	created_jobs integer := 0;
	c CURSOR FOR SELECT state.function_set_id, dest_timestamp, processed_max_modified
		FROM transform.state
		JOIN transform.function_set_tag_link fstl ON fstl.function_set_id = state.function_set_id
		JOIN directory.tag t ON t.id = fstl.tag_id
		JOIN system.job j ON j.id = job_id
		WHERE dest_timestamp < now()
			AND (max_modified > processed_max_modified OR processed_max_modified IS NULL)
			AND NOT j.finished IS NULL
			AND max_modified < now() - interval '60 seconds'
		FOR UPDATE;
BEGIN
	FOR r IN c LOOP
		SELECT transform.compile_transform_job(r.function_set_id, r.dest_timestamp);

		created_jobs := created_jobs + 1;
	END LOOP;

	RETURN created_jobs;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION add_array(int[], int[]) RETURNS int[]
AS $$
SELECT array_agg(arr1 + arr2) FROM
(
	SELECT
	unnest($1[1:least(array_length($1,1), array_length($2,1))]) AS arr1,
	unnest($2[1:least(array_length($1,1), array_length($2,1))]) AS arr2
) AS foo;
$$ LANGUAGE SQL STABLE STRICT;


CREATE AGGREGATE sum_array (int[])
(
    sfunc = add_array,
    stype = int[]
);


CREATE OR REPLACE FUNCTION add_array(double precision[], double precision[]) RETURNS double precision[]
AS $$
SELECT array_agg(arr1 + arr2) FROM
(
	SELECT
	unnest($1[1:least(array_length($1,1), array_length($2,1))]) AS arr1,
	unnest($2[1:least(array_length($1,1), array_length($2,1))]) AS arr2
) AS foo;
$$ LANGUAGE SQL STABLE STRICT;


CREATE AGGREGATE sum_array (double precision[])
(
    sfunc = add_array,
    stype = double precision[]
);


CREATE OR REPLACE FUNCTION multiply_array(int[], int[]) RETURNS int[]
AS $$
SELECT array_agg(arr1 * arr2) FROM
(
	SELECT
	unnest($1[1:least(array_length($1,1), array_length($2,1))]) AS arr1,
	unnest($2[1:least(array_length($1,1), array_length($2,1))]) AS arr2
) AS foo;
$$ LANGUAGE SQL STABLE STRICT;


CREATE OR REPLACE FUNCTION divide_array(int[], int) RETURNS double precision[]
AS $$
SELECT array_agg(arr / $2::double precision) FROM
(
	SELECT unnest($1) AS arr
) AS foo;
$$ LANGUAGE SQL STABLE STRICT;


CREATE OR REPLACE FUNCTION divide_array(int[], int[]) RETURNS double precision[]
AS $$
SELECT array_agg(transform.safe_division(arr1::double precision, arr2::double precision)) FROM
(
	SELECT
	unnest($1[1:least(array_length($1,1), array_length($2,1))]) AS arr1,
	unnest($2[1:least(array_length($1,1), array_length($2,1))]) AS arr2
) AS foo;
$$ LANGUAGE SQL STABLE STRICT;


CREATE OR REPLACE FUNCTION divide_array(real[], real[]) RETURNS real[]
AS $$
SELECT array_agg(transform.safe_division(arr1, arr2)) FROM
(
	SELECT
	unnest($1[1:least(array_length($1,1), array_length($2,1))]) AS arr1,
	unnest($2[1:least(array_length($1,1), array_length($2,1))]) AS arr2
) AS foo;
$$ LANGUAGE SQL STABLE STRICT;


CREATE OR REPLACE FUNCTION divide_array(double precision[], double precision[]) RETURNS double precision[]
AS $$
SELECT array_agg(transform.safe_division(arr1, arr2)) FROM
(
	SELECT
	unnest($1[1:least(array_length($1,1), array_length($2,1))]) AS arr1,
	unnest($2[1:least(array_length($1,1), array_length($2,1))]) AS arr2
) AS foo;
$$ LANGUAGE SQL STABLE STRICT;


CREATE OR REPLACE FUNCTION array_sum(int[]) RETURNS bigint
AS $$
SELECT sum(t) FROM unnest($1) t;
$$ LANGUAGE SQL STABLE STRICT;


CREATE DOMAIN pdf AS int[];


CREATE OR REPLACE FUNCTION to_pdf(text)
	RETURNS transform.pdf
AS $$
	SELECT array_agg(nullif(x, '')::int)::transform.pdf FROM unnest(string_to_array($1, ',')) AS x;
$$ LANGUAGE SQL STABLE STRICT;
