with 

pull_request as (
    select * from {{ ref('stg_github__pull_request') }}
), 

repository as (
    select * from {{ ref('stg_github__repository') }}
), 

issue as (
    select * from {{ ref('stg_github__issue') }}
), 

issue_merged as (
    select * from {{ ref('stg_github__issue_merged') }}
), 

final as (
    select 

        pull_request.pull_request_id,
        repository.name as repo_name,
        issue.number as pull_request_number,
        cast(null as string) as type,

        case when pull_request.is_draft is not null then 'draft'
            when issue_merged.merged_at is not null then 'merged'
            when issue.closed_at is not null then 'closed_without_merge'
            else 'open'
        end as state,

        issue.created_at as opened_at,
        issue_merged.merged_at,
        date_diff(issue_merged.merged_at, issue.created_at, hour) / 24.0 as days_open_to_merge

    from pull_request
    left join repository 
        on pull_request.head_repo_id = repository.repository_id 
    left join issue 
        on pull_request.issue_id = issue.issue_id
    left join issue_merged 
        on pull_request.issue_id = issue_merged.issue_id
)

select * from final