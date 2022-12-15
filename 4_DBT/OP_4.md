Analytical engineering:

DBT: 
setting dbt:
-preferred before installing: pip install --upgrade pip wheel setuptools (update ur tool)
- on terminal for local: pip install   dbt-core   dbt-postgres   dbt-redshift   dbt-snowflake   dbt-bigquery (choose what u want)
- or just connect ur bq and repo on dbt cloud (local data must use dbt core *cant use cloud)

profiles in: ~/.dbt/profiles.yml

dbt main/model/staging: schema.yml, SQL files (.sql)
    main/macros/ defined_fun.sql: Pre-define a block of code in it
    main/packages.yml: defined macros e.g dbt utils     ### dbt packages hub / github
    ::: dbt deps ::: to download dependencies
    define variables like any other language (can be set during command lines in run): var("Name", default = ), used: dbt build --m <model.sql> --var 'is_test_run: false'

dbt seeds: are csv files use tables with rest macros, smaller files that use data that does not change this often (e.g taxi_lookup table: a config table ouline variables of data)
::: dbt seed ::: create table of seed with data types correspond to it
    can add seed_prop.yml with description for seeds.

    model/core/dim_zones.sql : model for seed
    model/core/fact_trips.sql : Union all your models in it so you can run this file with its dependencies.

adding tests "assumptions" on your dbt with sql select statements in schema.yml (yml with running models)
:::dbt test:::

documentation hinted from videos and given notes...

Production:
    UI of dbt: create jobs with multiple commands to run,
        run projects in production schemas
    
    set-up Production Environment after commit your development, set your desired Jobs
    generate doc on run::: since we added documentation to our models; it will be generated

link your documentation artifact on your project settings

Data Visualization using Google Studio:

My Report output: https://datastudio.google.com/reporting/405b9707-af2d-4598-a790-abff5ff69e83
