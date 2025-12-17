create table processed_events_aggregated(
	event_hour TIMESTAMP(3),
    host VARCHAR,
    num_hits BIGINT
);


create table processed_events_aggregated_source(
	event_hour TIMESTAMP(3),
    host VARCHAR,
    referrer VARCHAR,
    num_hits BIGINT
);
