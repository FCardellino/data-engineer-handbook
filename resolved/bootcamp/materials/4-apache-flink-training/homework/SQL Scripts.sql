--CREATE TABLE TO SAVE DATA FOR TASK 1: Create a Flink job that sessionizes the input data by IP address and host

create table processed_events_aggregated_session(
	event_hour TIMESTAMP(3),
    ip VARCHAR,
    host VARCHAR,
    num_hits BIGINT
);


--Q1: What is the average number of web events of a session from a user on Tech Creator?
select 
ip as user,
avg(num_hits) as avg_num_web_events
from processed_events_aggregated_session
where host like '%techcreator%'
group by ip
order by avg(num_hits) desc;

--Compare results between different hosts (zachwilson.techcreator.io, zachwilson.tech, lulu.techcreator.io)
select 
host,
avg(num_hits) as avg_num_web_events
from processed_events_aggregated_session
where host like '%techcreator%'
group by host
order by avg(num_hits) desc; 


