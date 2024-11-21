## Getting started

## Glue Jobs

- [ ] You need to create Glue jobs from AWS UI.
- [ ] You can get code for Glue jobs from this repository.
- [ ] When creating Glue jobs please update all the paramerts in Glue jobs as per your confiugration and environment.

## Airflow Configuration

Before starting you need to set below mention configuration for airflow as per your environment.
- [ ] Create python virtual environment for your airflow.
```
python3 -m venv /path/to/new/virtual/environment
```
- [ ] After creating virtualenv setup environment variable. 
```
export AIRFLOW_HOME = <path_to_your_airflow_home_location>
export DBT_PROFILES_DIR = <path_to_dbt_project.yml>
export DBT_PROJECT_DIR = <path_to_profiles.yml>
export AWS_ACCESS_KEY_ID="<your_access_key>"
export AWS_SECRET_ACCESS_KEY="<your_secret_key>"
```
- [ ] For AWS access you can also set AWS profile.
- [ ] Also you need to update "airflow.cfg" file as per your environment. You need to update below mentioned configs
```
executor = LocalExecutor
sql_alchemy_conn = <set_it_to_aws_rds>

dags_folder = <path_to_your_airflow_environment>
plugins_folder = <path_to_your_airflow_environment>
base_log_folder = <path_to_your_airflow_environment>
dag_processor_manager_log_location = <path_to_your_airflow_environment>
child_process_log_directory = <path_to_your_airflow_environment>
```

## DBT Configuration
- [ ] You will need to setup "profiles.yml" file as per your Snowflake account.
```
dbt_transformations:
  outputs:
    dev:
      account: <snowflake_account_id>.<region>.<cloud provider>
      database: <snowflake_database>
      password: <snowflake_password>
      role: <snowflake_role>
      schema: <snowflake_schema>
      threads: 20
      type: snowflake
      user: <snowflake_user>
      warehouse: <snowflake_warehouse>
  target: dev
```
- [ ] Example of snowflake account : cr59721.us-east-2.aws
- [ ] You will also need to update "sources.yml" as per your database and table names that you define in snowflake.
```
version: 2
sources:
- name: FIVETRAN_SALES
  database: FIVETRAN_DATABASE
  schema: SALES
  tables:
    - name: DIM_ADDRESS
    - name: DIM_CREDITCARD
    - name: DIM_CUSTOMER
    - name: DIM_ORDERSTATUS
    - name: DIM_PRODUCT
    - name: STG_DATE
```
- [ ] After doing everything above you need to run "dbt deps" command to install packages.

## NOTE
- [ ] Before runing the pipeline please install everything in requirements.txt



## Add your files
To make it easy for you to get started with GitLab, here's a list of recommended next steps.

Already a pro? Just edit this README.md and make it your own. Want to make it easy? [Use the template at the bottom](#editing-this-readme)

- [ ] [Create](https://docs.gitlab.com/ee/user/project/repository/web_editor.html#create-a-file) or [upload](https://docs.gitlab.com/ee/user/project/repository/web_editor.html#upload-a-file) files
- [ ] [Add files using the command line](https://docs.gitlab.com/ee/gitlab-basics/add-file.html#add-a-file-using-the-command-line) or push an existing Git repository with the following command:

```
cd existing_repo
git remote add origin <url>
git branch -M main
git push -uf origin main
```