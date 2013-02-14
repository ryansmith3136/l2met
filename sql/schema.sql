create extension "uuid-osp";

create table tokens (
	id uuid,
	u text,
	p text,
	drain text
);

create unique index tokens_by_id on tokens(id);

create table buckets (
	id bigserial,
	token uuid,
	measure text,
	source text,
	time timestamptz,
	vals float8[] default '{}'
);

create unique index buckets_by_measure_source_time_token_bucket
on buckets(measure, source, time, token);
