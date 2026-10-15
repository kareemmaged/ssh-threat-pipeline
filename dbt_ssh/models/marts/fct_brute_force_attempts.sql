with failed as (
    select * from {{ ref('stg_ssh_events') }}
    where event_type = 'failed_password'
),
flagged as (
    select
        source_ip,
        username,
        event_ts,
        count(*) over (
            partition by source_ip
            order by event_ts
            range between interval '10 minutes' preceding and current row
        ) as failures_last_10_min
    from failed
)
select * from flagged where failures_last_10_min >= 5