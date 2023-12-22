with source as (

    select * from {{ source('github', 'repository') }}

),

renamed as (

    select
        id as repository_id,
        owner_id as owner_user_id,
        archived as is_archived,
        default_branch,
        description,
        fork as is_fork,
        full_name,
        homepage,
        language,
        name,
        private as is_private,
        created_at
    from source

)

select * from renamed