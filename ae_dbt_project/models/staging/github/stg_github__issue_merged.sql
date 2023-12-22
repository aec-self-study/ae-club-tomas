with source as (
    select *
    from {{ source('github', 'issue_merged') }}
)

, renamed as (
    select
        issue_id,
        actor_id as merge_user_id,
        commit_sha,
        merged_at
    from source
)

select * from renamed