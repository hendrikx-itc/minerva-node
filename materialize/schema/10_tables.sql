SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;

CREATE SCHEMA materialization;
ALTER SCHEMA materialization OWNER TO minerva_admin;

GRANT ALL ON SCHEMA materialization TO minerva_writer;
GRANT USAGE ON SCHEMA materialization TO minerva;

SET search_path = materialization, pg_catalog;


-- Table 'type'

CREATE TABLE type (
	id serial NOT NULL,
	src_trendstore_id integer NOT NULL,
	dst_trendstore_id integer NOT NULL,
	processing_delay interval NOT NULL,
	stability_delay interval NOT NULL,
	reprocessing_period interval NOT NULL,
	enabled boolean NOT NULL DEFAULT FALSE
);

ALTER TABLE type OWNER TO minerva_admin;

ALTER TABLE ONLY type
	ADD CONSTRAINT type_pkey PRIMARY KEY (id);

GRANT ALL ON TABLE type TO minerva_admin;
GRANT SELECT ON TABLE type TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE type TO minerva_writer;

ALTER TABLE ONLY type
	ADD CONSTRAINT materialization_type_src_trendstore_id_fkey
	FOREIGN KEY (src_trendstore_id) REFERENCES trend.trendstore(id)
	ON DELETE CASCADE;

ALTER TABLE ONLY type
	ADD CONSTRAINT materialization_type_dst_trendstore_id_fkey
	FOREIGN KEY (dst_trendstore_id) REFERENCES trend.trendstore(id)
	ON DELETE CASCADE;

CREATE UNIQUE INDEX ix_materialization_type_uniqueness
	ON type (src_trendstore_id, dst_trendstore_id);


-- table state

CREATE TYPE source_fragment AS (
	trendstore_id integer,
	timestamp timestamp with time zone
);


CREATE TYPE source_fragment_state AS (
	fragment source_fragment,
	modified timestamp with time zone
);

COMMENT ON TYPE source_fragment_state IS 'Used to store the max modified of a specific source_fragment.';


CREATE TABLE state (
	type_id integer NOT NULL,
	timestamp timestamp with time zone NOT NULL,
	max_modified timestamp with time zone NOT NULL,
	source_states source_fragment_state[] DEFAULT NULL,
	processed_states source_fragment_state[] DEFAULT NULL,
	job_id integer DEFAULT NULL
);

ALTER TABLE state OWNER TO minerva_admin;

ALTER TABLE ONLY state
	ADD CONSTRAINT state_pkey PRIMARY KEY (type_id, timestamp);

ALTER TABLE ONLY state
	ADD CONSTRAINT materialization_state_type_id_fkey
	FOREIGN KEY (type_id) REFERENCES materialization.type(id)
	ON DELETE CASCADE;

GRANT ALL ON TABLE state TO minerva_admin;
GRANT SELECT ON TABLE state TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE state TO minerva_writer;


-- Table 'type_tag_link'

CREATE TABLE type_tag_link (
    type_id integer NOT NULL,
    tag_id integer NOT NULL
);

ALTER TABLE type_tag_link OWNER TO minerva_admin;

ALTER TABLE ONLY type_tag_link
    ADD CONSTRAINT type_tag_link_pkey PRIMARY KEY (type_id, tag_id);

ALTER TABLE ONLY type_tag_link
    ADD CONSTRAINT type_tag_link_tag_id_fkey FOREIGN KEY (tag_id) REFERENCES directory.tag(id)
	ON DELETE CASCADE;

ALTER TABLE ONLY type_tag_link
	ADD CONSTRAINT type_tag_link_type_id_fkey FOREIGN KEY (type_id) REFERENCES type(id)
	ON DELETE CASCADE;

GRANT ALL ON TABLE type_tag_link TO minerva_admin;
GRANT SELECT ON TABLE type_tag_link TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE type_tag_link TO minerva_writer;
