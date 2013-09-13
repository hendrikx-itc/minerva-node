SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;

CREATE SCHEMA transform;
ALTER SCHEMA transform OWNER TO minerva_admin;

GRANT ALL ON SCHEMA transform TO minerva_writer;
GRANT USAGE ON SCHEMA transform TO minerva;

SET search_path = transform, pg_catalog;


-- Table 'function_set'

CREATE TABLE function_set (
	id serial NOT NULL,
	name varchar NOT NULL,
	description text,
	mapping_signature int[] NOT NULL,
	source_datasource_ids int[] NOT NULL,
	source_entitytype_id int NOT NULL,
	source_granularity varchar NOT NULL,
	dest_datasource_id int NOT NULL,
	dest_entitytype_id int NOT NULL,
	dest_granularity varchar NOT NULL,
	filter_sub_query text DEFAULT NULL,
	group_by varchar[] DEFAULT NULL,
	relation_type_id int DEFAULT NULL,
	enabled boolean DEFAULT FALSE,
	CONSTRAINT function_set_pkey PRIMARY KEY (id),
	CONSTRAINT function_set_source_entitytype_id_fkey FOREIGN KEY (source_entitytype_id)
		REFERENCES directory.entitytype (id) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE CASCADE,
	CONSTRAINT function_set_dest_entitytype_id_fkey FOREIGN KEY (dest_entitytype_id)
		REFERENCES directory.entitytype (id) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE CASCADE,
	CONSTRAINT function_set_dest_datasource_id_fkey FOREIGN KEY (dest_datasource_id)
		REFERENCES directory.datasource (id) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE CASCADE,
	CONSTRAINT function_set_relation_type_id_fkey FOREIGN KEY (relation_type_id)
		REFERENCES relation.type (id) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE CASCADE
);
COMMENT ON COLUMN transform.function_set.name IS 'Name of function set, e.g. kpi name';
COMMENT ON COLUMN transform.function_set.mapping_signature IS 'E.g. [{function_mapping_1, function_mapping_2}]';
COMMENT ON COLUMN transform.function_set.filter_sub_query IS 'Optional subquery for filtering entities (e.g. in case of staged software roll out)';

ALTER TABLE function_set OWNER TO minerva_admin;

ALTER TABLE function_set_id_seq OWNER TO minerva_admin;

ALTER TABLE function_set ALTER COLUMN id SET DEFAULT nextval('function_set_id_seq'::regclass);

ALTER SEQUENCE function_set_id_seq OWNED BY function_set.id;

GRANT ALL ON TABLE function_set TO minerva_admin;
GRANT SELECT ON TABLE function_set TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE function_set TO minerva_writer;

GRANT ALL ON SEQUENCE function_set_id_seq TO minerva_admin;
GRANT SELECT ON SEQUENCE function_set_id_seq TO minerva;
GRANT UPDATE ON SEQUENCE function_set_id_seq TO minerva_writer;

-- table function_mapping

CREATE TABLE function_mapping (
	id serial NOT NULL,
	function_name varchar,
	signature_columns varchar[] NOT NULL,
	dest_column varchar NOT NULL,
	CONSTRAINT function_mapping_pkey PRIMARY KEY (id)
);
COMMENT ON TABLE transform.function_mapping IS 'Table for defining mappings of function output to destination column';

ALTER TABLE function_mapping OWNER TO minerva_admin;
ALTER TABLE function_mapping_id_seq OWNER TO minerva_admin;
ALTER TABLE function_mapping ALTER COLUMN id SET DEFAULT nextval('function_set_id_seq'::regclass);
ALTER SEQUENCE function_mapping_id_seq OWNED BY function_set.id;

GRANT ALL ON TABLE function_mapping TO minerva_admin;
GRANT SELECT ON TABLE function_mapping TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE function_mapping TO minerva_writer;

GRANT ALL ON SEQUENCE function_mapping_id_seq TO minerva_admin;
GRANT SELECT ON SEQUENCE function_mapping_id_seq TO minerva;
GRANT UPDATE ON SEQUENCE function_mapping_id_seq TO minerva_writer;


--- table state

CREATE TABLE state (
	function_set_id integer NOT NULL,
	dest_timestamp timestamp with time zone NOT NULL,
	processed_max_modified timestamp with time zone,
	max_modified timestamp with time zone NOT NULL,
	job_id integer DEFAULT NULL,
	CONSTRAINT state_pkey PRIMARY KEY (function_set_id, dest_timestamp),
	CONSTRAINT function_set_id_fkey FOREIGN KEY (function_set_id)
		REFERENCES transform.function_set (id) MATCH SIMPLE
		ON UPDATE NO ACTION ON DELETE CASCADE
);

ALTER TABLE ONLY state
	ADD CONSTRAINT job_id_fkey FOREIGN KEY (job_id) REFERENCES system.job (id)
		MATCH SIMPLE ON UPDATE NO ACTION ON DELETE SET DEFAULT;

ALTER TABLE state OWNER TO minerva_admin;

GRANT ALL ON TABLE state TO minerva_admin;
GRANT SELECT ON TABLE state TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE state TO minerva_writer;

-- Table 'function_set_tag_link'

CREATE TABLE function_set_tag_link (
    function_set_id integer NOT NULL,
    tag_id integer NOT NULL
);

ALTER TABLE function_set_tag_link OWNER TO minerva_admin;

ALTER TABLE ONLY function_set_tag_link
    ADD CONSTRAINT function_set_tag_link_pkey PRIMARY KEY (function_set_id, tag_id);

ALTER TABLE ONLY function_set_tag_link
    ADD CONSTRAINT function_set_tag_link_tag_id_fkey FOREIGN KEY (tag_id) REFERENCES directory.tag(id)
	ON DELETE CASCADE;

ALTER TABLE ONLY function_set_tag_link
    ADD CONSTRAINT function_set_tag_link_trend_id_fkey FOREIGN KEY (function_set_id) REFERENCES function_set(id)
	ON DELETE CASCADE;

GRANT ALL ON TABLE function_set_tag_link TO minerva_admin;
GRANT SELECT ON TABLE function_set_tag_link TO minerva;
GRANT INSERT,DELETE,UPDATE ON TABLE function_set_tag_link TO minerva_writer;
