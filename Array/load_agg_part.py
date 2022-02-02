from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import argparse
from datetime import datetime

# Creating SparkSession
spark = SparkSession.builder.master("yarn").config("spark.sql.crossJoin.enabled", "true").enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("hive.exec.dynamic.partition.mode","nonstrict")

db='rtd_mart'

tbl_dim_year_month='dim_year_month'
tbl_trs_agg_corep_crd4 ='trs_agg_corep_crd4'
tbl_etl_parameter='etl_parameter'
tbl_agg_corep_crd4='agg_corep_crd4'
tbl_s_nq_ept='s_nq_ept'

def proc_load_agg_corep_crd4():
    v_reporting_date=get_reporting_date()
    print(v_reporting_date)
    
    rYear_Month_RK=get_year_month_rk(v_reporting_date)
    print(rYear_Month_RK)
               
    insert_agg_corep_crd4(v_reporting_date,rYear_Month_RK)
	
	
def get_reporting_date():
    query=f'''SELECT to_date(from_unixtime(unix_timestamp(parameter_value,"dd-MMM-yyyy")))  as reporting_date
    FROM {db}.{tbl_etl_parameter}  WHERE PARAMETER_NAME = 'REPORTING_DATE' '''
    
    reporting_date = spark.sql(query).collect()[0][0]
    return reporting_date

def get_year_month_rk(v_reporting_date):
    query=f'''SELECT YEAR_MONTH_RK FROM {db}.{tbl_dim_year_month} WHERE YEAR = year('{v_reporting_date}')
                   and MONTH= month('{v_reporting_date}') '''
    year_month_rk= spark.sql(query).collect()[0][0]
    return year_month_rk


def insert_agg_corep_crd4(v_reporting_date,rYear_Month_RK):
    v_cr_crd4_obiee_database = spark.sql(f'''SELECT parameter_value  FROM {db}.{tbl_etl_parameter}  WHERE  PARAMETER_NAME = 'OBIEE_CR_CRD4_DB' ''').collect()[0][0]
    
    print(v_cr_crd4_obiee_database)
    
    v_schema = spark.sql(f'''SELECT parameter_value  FROM {db}.{tbl_etl_parameter}  WHERE PARAMETER_NAME = 'FACT_SCHEMA' ''').collect()[0][0]
    print(v_schema)
    
    v_flag_type=spark.sql(f'''SELECT DISTINCT ADJUST_FLAG FROM {db}.{tbl_trs_agg_corep_crd4} ''').collect()[0][0]
    print(v_flag_type)
    
    dt_form=spark.sql(f'''select date_format('{v_reporting_date}' ,'YYYYMM')''').collect()[0][0]
    
    v_partition_name='P'+dt_form
    print(v_partition_name)
    
    if v_flag_type=='A':
        v_subpartition_name =v_partition_name+'_ADJUST'
    elif v_flag_type=='U':
        v_subpartition_name= v_partition_name+'_BASE'
    else:
        v_subpartition_name='NA'
        
    print(v_subpartition_name)
    
    add_partition_query=f'''ALTER TABLE {db}.{tbl_agg_corep_crd4} ADD IF NOT EXISTS PARTITION (year_month_rk={rYear_Month_RK},adjust_flag='{v_flag_type}')'''
    
    add_partition_df=spark.sql(add_partition_query)
    
    insert_df=spark.sql(f'''insert overwrite table {db}.{tbl_agg_corep_crd4} partition(YEAR_MONTH_RK={rYear_Month_RK},adjust_flag='{v_flag_type}') select 
 RRU_RK                          ,
  LE_COUNTERPARTY_RK              ,
  DIVISION_RK                     ,
  COREP_PRODUCT_CATEGORY_RK       ,
  PRODUCT_RK                      ,
  COREP_STD_ASSET_CLASS_RK        ,
  COREP_PRDEF_STD_ASSET_CLASS_RK  ,
  B2_STD_ASSET_CLASS_RK           ,
  COUNTERPARTY_MGS_PD_BAND_RK     ,
  UNSETTLED_PERIOD_CODE           ,
  UNSETTLED_FACTOR_CODE           ,
  CBI_PD_BAND_CODE_RK             ,
  INCORP_COUNTRY_RK               ,
  BRANCH_COUNTRY_RK               ,
  OP_COUNTRY_RK                   ,
  RISK_COUNTRY_RK                 ,
  MGS_PD_BAND_CODE_RK             ,
  RUN_RK                          ,
  REPORTING_REGULATOR_CODE        ,
  BASEL_DATA_FLAG                 ,
  RWA_APPROACH_CODE               ,
  ACLM_PRODUCT_TYPE               ,
  RETAIL_OFF_BAL_SHEET_FLAG       ,
  DEFAULT_FUND_CONTRIB_INDICATOR  ,
  FLOW_TYPE                       ,
  SME_FLAG                        ,
  INTERNAL_TRANSACTION_FLAG       ,
  IRB_RISK_WEIGHT_RATIO           ,
  STD_RISK_WEIGHT_RATIO           ,
  B2_STD_RISK_WEIGHT_INDICATOR    ,
  SLOTTING_RESIDUAL_MATURITY      ,
  RWA_CALC_METHOD                 ,
  LRFE_UFE_FLAG                   ,
  OBLIGOR_TYPE                    ,
  QCCP_FLAG                       ,
  DOUBLE_DEFAULT_FLAG             ,
  TRADING_BOOK_FLAG               ,
  REPORTING_TYPE_CODE             ,
  B3_CVA_CC_ADV_FLAG              ,
  B2_STD_CCF                      ,
  B2_STD_CCF_LESS_THAN_YEAR       ,
  B2_STD_CCF_MORE_THAN_YEAR       ,
  B2_IRB_OBSRVD_NEW_DEFAULT_FLAG  ,
  B2_STD_OBSRVD_NEW_DEFAULT_FLAG  ,
  B2_IRB_ORIG_EXP_PRE_CON_FACTOR  ,
  IRB_ORIG_EXP_PRE_CONV_ONBAL     ,
  IRB_ORIG_EXP_PRE_CONV_OFBAL     ,
  B2_STD_ORIG_EXP_PRE_CON_FACTOR  ,
  STD_ORIG_EXP_PRE_CONV_ONBAL     ,
  STD_ORIG_EXP_PRE_CONV_OFBAL     ,
  B3_CCP_MARGIN_INITL_CASH_AMT    ,
  B3_CCP_MARGIN_INITL_NOCASH_AMT  ,
  B3_CVA_PROVISION_AMT            ,
  B2_IRB_PROVISIONS_AMT           ,
  B2_STD_PROVISIONS_AMT           ,
  B2_STD_PROVISIONS_AMT_ONBAL     ,
  B2_STD_PROVISIONS_AMT_OFBAL     ,
  B2_IRB_CRM_UFCP_GUAR_AMT        ,
  B2_IRB_CRM_UFCP_CRED_DERIV_AMT  ,
  B2_IRB_CRM_FCP_ELG_FIN_COL_AMT  ,
  B2_IRB_CRM_FCP_OTH_PHY_COL_AMT  ,
  B2_IRB_CRM_FCP_OTH_FUNDED_AMT   ,
  B2_IRB_CRM_FCP_REAL_ESTATE_AMT  ,
  B2_IRB_CRM_FCP_RECEIVABLES_AMT  ,
  B2_IRB_CRM_UFCP_DOUBLE_DEF_AMT  ,
  B2_STD_EFF_GTEE_AMT             ,
  B2_STD_EFF_CDS_AMT              ,
  B2_STD_OTH_FUN_CRED_PROT_AMT    ,
  B2_STD_EFF_FIN_COLL_AMT         ,
  B2_STD_COLL_VOLATILITY_AMT      ,
  B2_STD_COLL_MTY_MISMATCH_AMT    ,
  B2_IRB_NETTED_COLL_GBP          ,
  B2_STD_NETTED_COLL_GBP          ,
  B2_STD_CRM_TOT_OUTFLOW          ,
  B2_STD_CRM_TOT_OUTFLOW_ONBAL    ,
  B2_STD_CRM_TOT_OUTFLOW_OFBAL    ,
  B2_STD_CRM_TOT_INFLOW           ,
  B2_STD_CRM_TOT_INFLOW_ONBAL     ,
  B2_STD_CRM_TOT_INFLOW_OFBAL     ,
  EAD_LGD_NUMERATOR               ,
  EAD_LGD_NUMERATOR_ON            ,
  EAD_LGD_NUMERATOR_OFF           ,
  EAD_MAT_YEARS_NUMERATOR         ,
  EAD_MAT_YEARS_NUMERATOR_ON      ,
  EAD_MAT_YEARS_NUMERATOR_OFF     ,
  EAD_PD_NUMERATOR                ,
  EAD_PD_NUMERATOR_ON             ,
  EAD_PD_NUMERATOR_OFF            ,
  EAD_POST_CRM_AMT                ,
  B2_IRB_EAD_AMT_ONBAL            ,
  B2_IRB_EAD_AMT_OFBAL            ,
  B2_APP_EXPECTED_LOSS_AMT        ,
  B2_IRB_EL_AMT_ONBAL             ,
  B2_IRB_EL_AMT_OFBAL             ,
  RWA_POST_CRM_AMOUNT             ,
  B2_IRB_RWA_AMT_ONBAL            ,
  B2_IRB_RWA_AMT_OFBAL            ,
  B3_CCP_MARGIN_INITL_RWA_AMT     ,
  B3_CCP_MARGIN_NONINITL_RWA_AMT  ,
  B1_RWA_POST_CRM_AMT             ,
  CORP_OBLIGOR_COUNT              ,
  RETAIL_OBLIGOR_COUNT            ,
  UNSETTLED_AMOUNT                ,
  UNSETTLED_PRICE_DIFFERENCE_AMT  ,
  B3_CVA_CC_ADV_VAR_AVG           ,
  B3_CVA_CC_ADV_VAR_SPOT          ,
  B3_CVA_CC_ADV_VAR_USED          ,
  B3_CVA_CC_ADV_VAR_STRESS_AVG    ,
  B3_CVA_CC_ADV_VAR_STRESS_SPOT   ,
  B3_CVA_CC_ADV_VAR_STRESS_USED   ,
  B3_CVA_CC_CREDIT_SPREAD_MEAN    ,
  B3_CVA_CC_CD_SNGL_NAM_NTNL_AMT  ,
  B3_CVA_CC_CDS_INDEX_NTNL_AMT    ,
  B3_CVA_CC_CAPITAL_AMT           ,
  B3_CVA_CC_RWA_AMT               ,
  B2_STD_NET_EXP_PRE_CRM_SUBST    ,
  STD_NET_EXP_PRECRM_SUBST_ONBAL  ,
  STD_NET_EXP_PRECRM_SUBST_OFBAL  ,
  B2_STD_NET_EXP_POST_CRM_SUBST   ,
  STD_NET_EXP_POSTCRM_SUBT_ONBAL  ,
  STD_NET_EXP_POSTCRM_SUBT_OFBAL  ,
  B2_STD_FULLY_ADJ_EXP            ,
  B2_STD_FULLY_ADJ_EXP_ONBAL      ,
  B2_STD_FULLY_ADJ_EXP_OFBAL      ,
  B2_STD_FULLY_ADJ_EXP_CCF_0      ,
  B2_STD_FULLY_ADJ_EXP_CCF_20     ,
  B2_STD_FULLY_ADJ_EXP_CCF_50     ,
  B2_STD_FULLY_ADJ_EXP_CCF_100    ,
  B2_STD_EXPOSURE_VALUE           ,
  B2_STD_EXPOSURE_VALUE_ONBAL     ,
  B2_STD_EXPOSURE_VALUE_OFBAL     ,
  B2_STD_EAD_POST_CRM_AMT_ONBAL   ,
  B2_STD_EAD_POST_CRM_AMT_OFBAL   ,
  B2_STD_RWA_AMT                  ,
  B2_STD_RWA_AMT_ONBAL            ,
  B2_STD_RWA_AMT_OFBAL            ,
  B2_STD_RWA_POST_CRM_AMT_ONBAL   ,
  B2_STD_RWA_POST_CRM_AMT_OFBAL   ,
  B2_STD_CRM_TOT_OUTFLOW_RWA      ,
  B2_STD_CRM_TOT_INFLOW_RWA       ,
  IMP_WRITE_OFF_AMT               ,
  IMP_IMPAIRMENT_AMT              ,
  B2_STD_COLL_FX_HAIRCUT_AMT      ,
  CRD4_REP_IRB_FLAG               ,
  CRD4_REP_IRBEQ_FLAG             ,
  CRD4_REP_STD_FLAG               ,
  CRD4_REP_STDFLOW_FLAG           ,
  CRD4_REP_SETTL_FLAG             ,
  CRD4_REP_CVA_FLAG               ,
  RISK_ON_RRU_RK                  ,
  B2_STD_EXP_HAIRCUT_ADJ_AMT      ,
  STD_EXP_TYPE_PRI_ORDER_RULE_RK  ,
  CRD4_SOV_SUBSTITUTION_FLAG      ,
  STD_RW_PRIORITY_ORDER_RULE_RK   ,
  UNLIKELY_PAY_FLAG               ,
  COUNTERPARTY_RK                 ,
  STD_EXPOSURE_DEFAULT_FLAG       ,
  CRD4_STD_RP_SME_FLAG            ,
  CRD4_IRB_RP_SME_FLAG            ,
  STD_SME_DISCOUNT_APP_FLAG       ,
  IRB_SME_DISCOUNT_APP_FLAG       ,
  TRANSITIONAL_PORTFOLIO_FLAG     ,
  SLOTTING_EFF_MATURITY_BAND_RK   ,
  COREP_IRB_ASSET_CLASS_RK        ,
  COREP_CRM_REPORTING_FLAG        ,
  ALTERNATIVE_STD_CCF_0           ,
  ALTERNATIVE_STD_CCF_20          ,
  ALTERNATIVE_STD_CCF_50          ,
  ALTERNATIVE_STD_CCF_100         ,
  COLL_GOOD_PROV_AMT              ,
  B2_IRB_INTEREST_AT_DEFAULT_GBP  ,
  PERS_ACC_FLAG                   ,
  B3_SME_RP_FLAG                  ,
  B3_SME_DISCOUNT_FLAG            ,
  STATUTORY_LEDGER_BALANCE        ,
  CARRYING_VALUE                  ,
  LEVERAGE_EXPOSURE               ,
  P3_PD_BAND_RK                   ,
  EXP_SECURED_BY_COLLATERAL_AMT   ,
  EXP_SEC_BY_COLL_COVERED_AMT     ,
  EXP_SECURED_BY_GUARANTEES_AMT   ,
  EXP_SEC_BY_GUAR_COVERED_AMT     ,
  EXP_SECURED_BY_CRD_DRVS_AMT     ,
  EXP_SEC_BY_CRD_DRV_COVERED_AMT  ,
  P3_STD_ASSET_CLASS_RK           ,
  BLANK_CRM_CATEGORY_AMT          ,
  EXP_UNSECURED_CARRYING_AMT      ,
  CRD4_MEMO_ITEMS_ASSET_CLASS_RK  ,
  INTEREST_SUSPENSE_AMT_GBP       ,
  TOT_REGU_PROV_AMT_GBP           ,
  COREP_INCORP_COUNTRY_RK         ,
  NET_MKT_VAL                     ,
  B2_APP_CAP_REQ_POST_CRM_AMT     ,
  LER_CHILD_COUNTERPARTY_RK       ,
  IG_EXCLUSION_FLAG               ,
  NEW_IN_DEFAULT_FLAG             ,
  WALKER_COST_CENTRE_RK           ,
  RETAIL_POOL_SME_FLAG            ,
  SYS_CODE                        ,
  CS_PROXY_USED_FLAG              ,
  MARKET_RISK_SUB_TYPE            ,
  ACCRUED_INTEREST_GBP            ,
  ACCRUED_INT_ONBAL_AMT           ,
  B2_IRB_RWA_PRE_CDS              ,
  DEAL_TYPE                       ,
  DRAWN_LEDGER_BALANCE            ,
  EAD_PRE_CRM_AMT                 ,
  GROSS_CARRYING_VALUE            ,
  IMPAIRMENT_LOSS_GBP             ,
  OFF_BALANCE_EXPOSURE            ,
  OFF_BAL_LED_EXP_GBP_POST_SEC    ,
  ON_BALANCE_LEDGER_EXPOSURE      ,
  ON_BAL_LED_EXP_GBP_POST_SEC     ,
  PROVISION_AMOUNT_GBP            ,
  RA_AD_EXP                       ,
  UNDRAWN_COMMITMENT_AMT          ,
  FSA_GROSS_EXPOSURE              ,
  FSA045_PD_BAND_RK               ,
  ULTIMATE_RING_FENCE_PARENT_RK   ,
  STS_SECURITISATION              ,
  STS_SEC_QUAL_CAP_TRTMNT         ,
  STS_SEC_APPROACH_CODE           ,
  B3_APP_RWA_INC_CVA_CC_AMT       ,
  IFRS9_TOT_REGU_ECL_AMT_GBP      ,
  IFRS9_FINAL_ECL_MES_GBP         ,
  IFRS9_FINAL_STAGE               ,
  IFRS9_STAGE_3_TYPE              ,
  IFRS9_FINAL_IIS_GBP             ,
  IFRS9_PROV_IMPAIRMENT_AMT_GBP   ,
  IFRS9_ECL_CURRENCY_CODE         ,
  IFRS9_PROV_WRITE_OFF_AMT_GBP    ,
  IFRS9_TOT_STATUT_ECL_AMT_GBP    ,
  IFRS9_DISCNT_UNWIND_AMT_GBP     ,
  RISK_TYPE                       ,
  NGAAP_PROVISION_AMT_GBP         ,
  REP_PROVISION_AMT_GBP           ,
  C33_EXPOSURE_SECTOR             ,
  FINREP_COUNTERPARTY_SECTOR      ,
  IAS39_CLASS                     ,
  REP_PROVISION_TYPE              ,
  B2_STD_RWA_PRE_SPRT_AMT         ,
  B2_STD_RWA_PRE_SPRT_AMT_ONBAL   ,
  B2_STD_RWA_PRE_SPRT_AMT_OFBAL   ,
  RWA_POST_CRM_PRE_SPRT_AMOUNT    ,
  B2_IRB_RWA_PRE_SPRT_AMT_ONBAL   ,
  B2_IRB_RWA_PRE_SPRT_AMT_OFBAL   ,
  CRR2_501A_DISCOUNT_FLAG         ,
  MORTGAGES_FLAG                  ,
  SECURITY_RK                     ,
  B2_IRB_CRM_COD_AMT              ,
  B2_IRB_CRM_LIP_AMT              ,
  BASEL_SECURITY_SUB_TYPE         ,
  SYS_PROTECTION_TYPE             
  from {db}.{tbl_trs_agg_corep_crd4}''')
    
    insert_s_nq_ept_df=spark.sql(f'''INSERT INTO  rtd_mart.s_nq_ept SELECT 1,current_date(),'{v_cr_crd4_obiee_database}',null,'{v_schema}','{tbl_agg_corep_crd4}',null''')	


