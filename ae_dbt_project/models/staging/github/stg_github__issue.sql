with source as (

    select * from {{ source('github', 'issue') }}

),

renamed as (

    select
        id as issue_id,
        user_id,
        body,
        locked,
        milestone_id,
        number,
        pull_request,
        repository_id,
        state,
        title,
        updated_at,
        closed_at,
        created_at

    from source

)

select * from renamed