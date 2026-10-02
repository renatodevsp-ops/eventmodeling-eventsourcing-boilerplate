CREATE TABLE public.events (
	id int8 DEFAULT nextval('events_id_seq'::regclass) NOT NULL,
	event_id uuid NOT NULL,
	stream_id varchar NOT NULL,
	stream_position int8 NOT NULL,
	event_type varchar NOT NULL,
	payload bytea NOT NULL,
	metadata bytea NULL,
	occurred_at timestamptz NOT NULL,
	CONSTRAINT events_pkey PRIMARY KEY (id),
	CONSTRAINT events_stream_id_stream_position_key UNIQUE (stream_id, stream_position)
);
CREATE INDEX idx_events_stream_id ON public.events USING btree (stream_id);

CREATE TABLE public.event_subscriptions (
	"name" varchar NOT NULL,
	"position" int8 DEFAULT 0 NOT NULL,
	CONSTRAINT event_subscriptions_pkey PRIMARY KEY (name)
);