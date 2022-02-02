CREATE EXTERNAL TABLE rtd_mart.dim_dpm_template_Stg(
TEMPLATE_RK int,
TEMPLATE_CODE string,
TEMPLATE_DESCRIPTION string,
DIM1_DOMAIN_RK int,
DIM2_DOMAIN_RK int,
DIM3_DOMAIN_RK int,
DIM4_DOMAIN_RK int,
DIM5_DOMAIN_RK int,
DIM6_DOMAIN_RK int,
DIM7_DOMAIN_RK int,
DIM8_DOMAIN_RK int,
DIM9_DOMAIN_RK int,
DIM10_DOMAIN_RK int,
GROUP_DIM1_DOMAIN int,
VALID_FLAG string,
VALID_FROM_DATE string,
VALID_TO_DATE string,
RUN_RK int,
REGULATION string,
SHEET_DOMAIN_RK int
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ESCAPED BY '\\' LINES TERMINATED BY '\n'
LOCATION
  's3://bucket-eu-west-1-897144472116-processed-data/transformation/rtd/rtd_mart/raw_dim_dpm_template/'
  tblproperties("skip.header.line.count"="1");
  
create table rtd_mart.dim_dpm_template(
TEMPLATE_RK int,
TEMPLATE_CODE string,
TEMPLATE_DESCRIPTION String,
DIM1_DOMAIN_RK int,
DIM2_DOMAIN_RK int,
DIM3_DOMAIN_RK int,
DIM4_DOMAIN_RK int,
DIM5_DOMAIN_RK int,
DIM6_DOMAIN_RK int,
DIM7_DOMAIN_RK int,
DIM8_DOMAIN_RK int,
DIM9_DOMAIN_RK int,
DIM10_DOMAIN_RK int,
GROUP_DIM1_DOMAIN int,
VALID_FLAG string,
VALID_FROM_DATE date,
VALID_TO_DATE date,
RUN_RK int,
REGULATION string,
SHEET_DOMAIN_RK int)
stored as parquet
LOCATION
  's3://bucket-eu-west-1-897144472116-processed-data/transformation/rtd/rtd_mart/dim_dpm_template/'
  
insert into  rtd_mart.dim_dpm_template
select 
cast(TEMPLATE_RK as int) ,
trim(template_code),
trim(TEMPLATE_DESCRIPTION),
cast(DIM1_DOMAIN_RK as int),
cast(DIM2_DOMAIN_RK as int),
cast(DIM3_DOMAIN_RK as int),
cast(DIM4_DOMAIN_RK as int),
cast(DIM5_DOMAIN_RK as int),
cast(DIM6_DOMAIN_RK as int),
cast(DIM7_DOMAIN_RK as int),
cast(DIM8_DOMAIN_RK as int),
cast(DIM9_DOMAIN_RK as int),
cast(DIM10_DOMAIN_RK as int),
cast(GROUP_DIM1_DOMAIN as int),
trim(VALID_FLAG),
cast(date_parse(substr(VALID_FROM_DATE,1,10),'%d/%m/%Y') as date) as VALID_FROM_DATE,
cast(date_parse(substr(VALID_TO_DATE,1,10),'%d/%m/%Y') as date) as VALID_TO_DATE,
cast(RUN_RK as int),
trim(REGULATION),
cast(SHEET_DOMAIN_RK as int) from rtd_mart.dim_dpm_template_Stg;


CREATE EXTERNAL TABLE rtd_mart.dim_dpm_template_row_col_Stg(
ROW_RK int,
TEMPLATE_RK int,
ROW_ID string,
ROW_TEXT1 string,
ROW_TEXT2 string,
ROW_TEXT3 string,
PLACEHOLDER_FLAG string,
SORT_ORDER int,
VALID_FLAG string,
VALID_FROM_DATE string,
VALID_TO_DATE string,
RUN_RK string,
REGU_TAXONOMY_EFFECTIVE_FROM string,
REGU_TAXONOMY_EFFECTIVE_TO string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ESCAPED BY '\\' LINES TERMINATED BY '\n'
LOCATION
  's3://bucket-eu-west-1-897144472116-processed-data/transformation/rtd/rtd_mart/raw_dim_dpm_template_row_col/'
  tblproperties("skip.header.line.count"="1");
  
create table rtd_mart.dim_dpm_template_row_col(
ROW_RK int,
TEMPLATE_RK int,
ROW_ID string,
ROW_TEXT1 string,
ROW_TEXT2 string,
ROW_TEXT3 string,
PLACEHOLDER_FLAG string,
SORT_ORDER int,
VALID_FLAG string,
VALID_FROM_DATE date,
VALID_TO_DATE date,
RUN_RK string,
REGU_TAXONOMY_EFFECTIVE_FROM date,
REGU_TAXONOMY_EFFECTIVE_TO date)
stored as parquet
LOCATION
  's3://bucket-eu-west-1-897144472116-processed-data/transformation/rtd/rtd_mart/dim_dpm_template_row_col/'
  ;
  
insert into  rtd_mart.dim_dpm_template_row_col
select 
ROW_RK ,
TEMPLATE_RK ,
ROW_ID ,
ROW_TEXT1 ,
ROW_TEXT2 ,
ROW_TEXT3 ,
PLACEHOLDER_FLAG ,
SORT_ORDER ,
VALID_FLAG ,
cast(date_parse(substr(VALID_FROM_DATE,1,10),'%d/%m/%Y') as date) as VALID_FROM_DATE ,
cast(date_parse(substr(VALID_TO_DATE ,1,10),'%d/%m/%Y') as date) as VALID_TO_DATE ,
RUN_RK ,
RUN_RK ,
cast(date_parse(substr(REGU_TAXONOMY_EFFECTIVE_FROM ,1,10),'%d/%m/%Y') as date) as REGU_TAXONOMY_EFFECTIVE_FROM ,
cast(date_parse(substr(REGU_TAXONOMY_EFFECTIVE_TO  ,1,10),'%d/%m/%Y') as date) as REGU_TAXONOMY_EFFECTIVE_TO 
from rtd_mart.dim_dpm_template_row_col_Stg;

-----------------------------------------------------------------------------------------------------------------------------------

CREATE EXTERNAL TABLE rtd_mart.dim_dpm_validation_rules_Stg(
VALIDATION_RULE_RK int,
EBA_VAL_ID string,
EBA_RELEASE string,
EBA_NOT_IMPL_IN_XBRL string,
EBA_VALIDATION_TYPE string,
EBA_SEVERITY string,
EBA_T1 string,EBA_T2 string,
EBA_T3 string,
EBA_T4 string,
EBA_T5 string,
EBA_T6 string,
EBA_T7 string,
EBA_ROWS string,
EBA_COLUMNS string,
EBA_SHEETS string,
EBA_FORMULA string,
T1 string,
T2 string,
T3 string,
T4 string,
T5 string,
T6 string,
T7 string,
REPORTING_ROW_ID string,
REPORTING_COLUMN_ID string,
SHEET string,
LHS_FORMULA string,
COMPARISON_OPERATOR string,
RHS_FORMULA string,
VALID_FLAG string,
VALID_FROM_DATE string,
VALID_TO_DATE string , 
RUN_RK string,
INT_EXT_IDENTIFIER string,
OVERRIDDEN_THRESHOLD_AMT string,
VALIDATION_CATEGORY string,
REGU_TAXONOMY_EFFECTIVE_FROM string,  
REGU_TAXONOMY_EFFECTIVE_TO string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ESCAPED BY '\\' LINES TERMINATED BY '\n'
LOCATION
  's3://bucket-eu-west-1-897144472116-processed-data/transformation/rtd/rtd_mart/raw_dim_dpm_validation_rules/'
  tblproperties("skip.header.line.count"="1");
  

create table rtd_mart.dim_dpm_validation_rules(
VALIDATION_RULE_RK int,
EBA_VAL_ID string,
EBA_RELEASE string,
EBA_NOT_IMPL_IN_XBRL string,
EBA_VALIDATION_TYPE string,
EBA_SEVERITY string,
EBA_T1 string,EBA_T2 string,
EBA_T3 string,
EBA_T4 string,
EBA_T5 string,
EBA_T6 string,
EBA_T7 string,
EBA_ROWS string,
EBA_COLUMNS string,
EBA_SHEETS string,
EBA_FORMULA string,
T1 string,
T2 string,
T3 string,
T4 string,
T5 string,
T6 string,
T7 string,
REPORTING_ROW_ID string,
REPORTING_COLUMN_ID string,
SHEET string,
LHS_FORMULA string,
COMPARISON_OPERATOR string,
RHS_FORMULA string,
VALID_FLAG string,
VALID_FROM_DATE date,
VALID_TO_DATE date,
RUN_RK string,
INT_EXT_IDENTIFIER string,
OVERRIDDEN_THRESHOLD_AMT string,
VALIDATION_CATEGORY string,
REGU_TAXONOMY_EFFECTIVE_FROM string,
REGU_TAXONOMY_EFFECTIVE_TO string)
stored as parquet
LOCATION
  's3://bucket-eu-west-1-897144472116-processed-data/transformation/rtd/rtd_mart/dim_dpm_validation_rules/'
 ;
 
 
insert into  rtd_mart.dim_dpm_validation_rules
select 
VALIDATION_RULE_RK ,
EBA_VAL_ID ,
EBA_RELEASE ,
EBA_NOT_IMPL_IN_XBRL ,
EBA_VALIDATION_TYPE ,
EBA_SEVERITY ,
EBA_T1 ,EBA_T2 ,
EBA_T3 ,
EBA_T4 ,
EBA_T5 ,
EBA_T6 ,
EBA_T7 ,
EBA_ROWS ,
EBA_COLUMNS ,
EBA_SHEETS ,
EBA_FORMULA ,
T1 ,
T2 ,
T3 ,
T4 ,
T5 ,
T6 ,
T7 ,
REPORTING_ROW_ID ,
REPORTING_COLUMN_ID ,
SHEET ,
LHS_FORMULA ,
COMPARISON_OPERATOR ,
RHS_FORMULA ,
VALID_FLAG ,
cast(date_parse(substr(VALID_FROM_DATE,1,10),'%d/%m/%Y') as date) as VALID_FROM_DATE ,
cast(date_parse(substr(VALID_TO_DATE,1,10),'%d/%m/%Y') as date) as VALID_TO_DATE , 
RUN_RK ,
INT_EXT_IDENTIFIER ,
OVERRIDDEN_THRESHOLD_AMT ,
VALIDATION_CATEGORY ,
REGU_TAXONOMY_EFFECTIVE_FROM ,  
REGU_TAXONOMY_EFFECTIVE_TO  
from rtd_mart.dim_dpm_validation_rules_Stg;

