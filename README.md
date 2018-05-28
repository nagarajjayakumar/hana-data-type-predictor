# hana-data-type-predictor


use maven to build the hana data type predictor code

Pass test skip flag in case if you need to skip Test case execution during install phase

mvn clean install -Dmaven.test.skip=true

# Inference Engine நாரை [Naarai] 

Following parameters need to be passed as argument to the Inference Engine 
Pass HANADB_HOST as system level environment variable

@minimal we need the following parameters

#### src_dbo_name => source database object name
#### analytic_type => source db type {hana | oracle | mysql}
#### mdbenv => Schema crawler metadata db environment
#### mdbservice => Schema crawler metadata db service {Needed for Skinny CURD framework}
#### runtime_env => Spark Runtime environment  {local | yarn [client | cluster]}
#### src_name_space => source name space {db name | schema name}
#### smpl_techniq => sampling technique {STARTIFIED_RESERVOIR_SAMPLING | STRATIFIED_CONSTANT_PROPORTION | STARTIFIED_DYNAMIC_POPULATION | RANDOM_UNIFORM_SYSTEM | RANDOM_UNIFORM_BERNOULI | NO_SAMPLING}
#### smpl_percentage => sampling percentage
#### smpl_size => sampling size

```
--task schema_crawler_master --src_dbo_name "<<hana_db_namespace/hana_dbo_name>>" \
--analytic_type hana --mdbenv development_mysql --mdbservice service --runtime_env local \
--src_name_space _SYS_BIC --smpl_techniq STARTIFIED_RESERVOIR_SAMPLING --smpl_percentage 10 \
--smpl_size 500
```

Additional properties supported by app.

```
--task schema_crawler_master --src_dbo_name "<<hana_db_namespace/hana_dbo_name>>" \
--output /tmp --write_mode csv --analytic_type hana \
--mdbenv development_mysql --mdbservice service --runtime_env local \
--src_name_space _SYS_BIC --smpl_techniq STARTIFIED_RESERVOIR_SAMPLING --smpl_percentage 10 \
--smpl_size 500
```


# Schema Crawler தேனீ [teani] 

Following parameters need to be passed as argument to the schema crawler 
Pass HANADB_HOST as system level environment variable

@minimal we need the following parameters

#### src_dbo_name => source database object name
#### analytic_type => source db type {hana | oracle | mysql}
#### mdbenv => Schema crawler metadata db environment
#### mdbservice => Schema crawler metadata db service {Needed for Skinny CURD framework}
#### runtime_env => Spark Runtime environment  {local | yarn [client | cluster]}
#### src_name_space => source name space {db name | schema name}

```
--task schema_crawler_master --src_dbo_name "<<hana_db_namespace/hana_dbo_name>>" \
--analytic_type hana --mdbenv development_mysql --mdbservice service --runtime_env local \
--src_name_space _SYS_BIC
```

Additional properties supported by app.

```
--task schema_crawler_master --src_dbo_name "<<hana_db_namespace/hana_dbo_name>>" \
--output /tmp --write_mode csv --analytic_type hana \
--mdbenv development_mysql --mdbservice service --runtime_env local \
--src_name_space _SYS_BIC 
```


##### SIGN: NAGA JAY
##### CICD: Yogeshprabhu @yogesh
