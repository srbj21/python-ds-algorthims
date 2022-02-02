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
