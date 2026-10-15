with source as (
    select * from {{ source('secops', 'raw_ssh_events') }}
),
classified as (
    select
        id,
        event_ts,
        source_ip,
        username,
        event_type,
        source_port,
        raw_message
    from source
    where event_ts is not null
)
select * from classified