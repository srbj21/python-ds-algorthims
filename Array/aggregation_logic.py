from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import argparse
from datetime import datetime

# Creating SparkSession
spark = SparkSession.builder.master("yarn").config("spark.sql.crossJoin.enabled", "true").enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("hive.exec.dynamic.partition.mode","nonstrict")


if __name__ == "__main__":
	args_parser = argparse.ArgumentParser()
	args_parser.add_argument("--reporting_date", required=True, type=int)
	args = args_parser.parse_args()
	V_DAY_RK = args.reporting_date
	start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
	
	latest_run = spark.sql(""" select max(consolidated_id) from rtd_shared_trans.dim_consolidated_run """).collect()[0][0]
	
	dim_control_start = f""" INSERT INTO rtd_shared_trans.dim_control 
							select 
							cast('{latest_run}' as bigint) as run_id,
							cast(to_date(from_unixtime(unix_timestamp(cast({V_DAY_RK} as string) ,'yyyyMMdd'))) as date) as business_date,
							cast('STARTED' as string) as status,
							cast('{start_time}' as timestamp) as start_time,
							cast(null as timestamp) as end_time """
	
	dim_started_df = spark.sql(dim_control_start)
							
	
	aggregation_query = f""" insert into rtd_shared_trans.agg_exposure_l1 partition(run_id)
					select 
					FCM.AS_AT_DATE,
					SUM(FCM.AT_ENTRY_BAL_AMT)  as AT_ENTRY_BAL_AMT,
					SUM(FCM.B1_EAD_POST_CRM_AMT) as B1_EAD_POST_CRM_AMT,
					SUM(FCM.B1_EAD_PRE_CRM_AMT) as B1_EAD_PRE_CRM_AMT,
					SUM(FCM.B1_RWA_POST_CRM_AMT) as B1_RWA_POST_CRM_AMT,
					SUM(FCM.B1_RWA_PRE_CRM_AMT) as B1_RWA_PRE_CRM_AMT,
					SUM(FCM.B1_UNDRAWN_COMMITMENT_AMT_GBP) as B1_UNDRAWN_COMMITMENT_AMT_GBP,
					FCM.B2_APPROVED_APPROACH_CODE,
					INP.B2_APP_ADJUSTED_LGD_RATIO AS B2_APP_ADJUSTED_LGD_RATIO,
					cast(INP.B2_APP_ADJUSTED_PD_RATIO as decimal(12,7)) AS B2_APP_ADJUSTED_PD_RATIO,
					SUM(FCM.B2_APP_EAD_POST_CRM_AMT) as B2_APP_EAD_POST_CRM_AMT,
					SUM(FCM.B2_APP_EAD_PRE_CRM_AMT) as B2_APP_EAD_PRE_CRM_AMT,
					SUM(FCM.B2_APP_EXPECTED_LOSS_AMT) as B2_APP_EXPECTED_LOSS_AMT,
					case 
						when SUM(FCM.B2_APP_EXPECTED_LOSS_AMT) = 0 or SUM(FCM.B2_APP_EXPECTED_LOSS_AMT) is null 
							 then AVG(FCM.B2_APP_EXPECTED_LOSS_RATIO)
						else
							 SUM(FCM.B2_APP_EXPECTED_LOSS_RATIO * FCM.B2_APP_EXPECTED_LOSS_AMT)/SUM(FCM.B2_APP_EXPECTED_LOSS_AMT)
					END AS B2_APP_EXPECTED_LOSS_RATIO,
					SUM(FCM.B2_APP_RWA_POST_CRM_AMT) as B2_APP_RWA_POST_CRM_AMT,
					SUM(FCM.B2_APP_RWA_POST_CRM_PRE_UPLIFT) as B2_APP_RWA_POST_CRM_PRE_UPLIFT,
					SUM(FCM.B2_APP_RWA_PRE_CRM_AMT) as B2_APP_RWA_PRE_CRM_AMT,
					INP.B2_APP_UNADJUSTED_LGD_RATIO,
					SUM(FCM.B2_FIRB_EAD_POST_CRM_AMT) as B2_FIRB_EAD_POST_CRM_AMT,
					SUM(FCM.B2_FIRB_EAD_PRE_CRM_AMT) as B2_FIRB_EAD_PRE_CRM_AMT,
					INP.B2_IRB_ADJUSTED_LGD_RATIO,
					cast(INP.B2_IRB_ADJUSTED_PD_RATIO as decimal(12,7)) as B2_IRB_ADJUSTED_PD_RATIO,
					SUM(FCM.B2_IRB_EAD_POST_CRM_AMT) as B2_IRB_EAD_POST_CRM_AMT,
					SUM(FCM.B2_IRB_EAD_PRE_CRM_AMT) as B2_IRB_EAD_PRE_CRM_AMT,
					case 
						when SUM(FCM.B2_IRB_EAD_POST_CRM_AMT) = 0 or SUM(FCM.B2_IRB_EAD_POST_CRM_AMT) is null 
							then AVG(FCM.B2_IRB_EFFECTIVE_MATURITY_YRS)
						else
							SUM(FCM.B2_IRB_EFFECTIVE_MATURITY_YRS * FCM.B2_IRB_EAD_POST_CRM_AMT)/SUM(FCM.B2_IRB_EAD_POST_CRM_AMT)
					END AS B2_IRB_EFFECTIVE_MATURITY_YRS,
					SUM(FCM.B2_IRB_EXPECTED_LOSS_AMT) as B2_IRB_EXPECTED_LOSS_AMT,
					case 
						when SUM(FCM.B2_IRB_EXPECTED_LOSS_AMT) = 0 or SUM(FCM.B2_IRB_EXPECTED_LOSS_AMT) is null 
								then AVG(FCM.B2_IRB_EXPECTED_LOSS_RATIO)
						else
							 SUM(FCM.B2_IRB_EXPECTED_LOSS_RATIO * FCM.B2_IRB_EXPECTED_LOSS_AMT)/SUM(FCM.B2_IRB_EXPECTED_LOSS_AMT) 
					END AS B2_IRB_EXPECTED_LOSS_RATIO,
					SUM(FCM.B2_IRB_RWA_POST_CRM_AMT) as B2_IRB_RWA_POST_CRM_AMT,
					SUM(FCM.B2_IRB_RWA_POST_CRM_PRE_UPLIFT) as B2_IRB_RWA_POST_CRM_PRE_UPLIFT,
					SUM(FCM.B2_IRB_RWA_PRE_CRM_AMT) as B2_IRB_RWA_PRE_CRM_AMT,
					INP.B2_IRB_UNADJUSTED_LGD_RATIO ,
					SUM(FCM.B2_STD_EAD_POST_CRM_AMT) as B2_STD_EAD_POST_CRM_AMT,
					SUM(FCM.B2_STD_EAD_PRE_CRM_AMT) as B2_STD_EAD_PRE_CRM_AMT,
					SUM(FCM.B2_STD_RWA_POST_CRM_AMT) as B2_STD_RWA_POST_CRM_AMT,
					SUM(FCM.B2_STD_RWA_PRE_CRM_AMT) as B2_STD_RWA_PRE_CRM_AMT,
					SUM(FCM.B2_UNDRAWN_COMMITMENT_AMT) as B2_UNDRAWN_COMMITMENT_AMT,
					FCM.COREP_PRODUCT_CATEGORY,
					cast(FCM.DAY_RK as bigint) as DAY_RK,
					FCM.DEFAULTED_EXPOSURE_FLAG as DEFAULTED_ASSET_FLAG,
					SUM(FCM.OFF_BALANCE_EXPOSURE) as OFF_BALANCE_EXPOSURE, 
					SUM(FCM.ON_BALANCE_LEDGER_EXPOSURE) as ON_BALANCE_LEDGER_EXPOSURE,
					FCM.ORIGINAL_CURRENCY_CODE,
					cast(INP.PD_RATIO_CALC_UNADJUSTED as decimal(12,7)) as PD_RATIO_CALC_UNADJUSTED,
					SUM(FCM.PROVISIONS_AMT) as PROVISION_AMOUNT_GBP,
					case 
						when SUM(FCM.B2_IRB_EAD_POST_CRM_AMT) = 0 or SUM(FCM.B2_IRB_EAD_POST_CRM_AMT) is null 
							  then AVG(FCM.RESIDUAL_MATURITY_DAYS)
						else
							SUM(FCM.RESIDUAL_MATURITY_DAYS * FCM.B2_IRB_EAD_POST_CRM_AMT)/SUM(FCM.B2_IRB_EAD_POST_CRM_AMT) 
					END AS RESIDUAL_MATURITY_DAYS,
					case 
						when SUM(FCM.B2_IRB_EAD_POST_CRM_AMT) = 0 or SUM(FCM.B2_IRB_EAD_POST_CRM_AMT) is null 
							then AVG(FCM.RESIDUAL_MATURITY_YEARS)
						else
							SUM(FCM.RESIDUAL_MATURITY_YEARS * FCM.B2_IRB_EAD_POST_CRM_AMT)/SUM(FCM.B2_IRB_EAD_POST_CRM_AMT) 
					END AS RESIDUAL_MATURITY_YEARS,
					FCM.src_sys_code as SYS_CODE, 
					FCM.POOLED_EXPOSURE_ID, 
					FCM.COUNTRY_OF_INCORPORATION,
					case 
						when SUM(FCM.B2_IRB_EAD_POST_CRM_AMT) = 0 or SUM(FCM.B2_IRB_EAD_POST_CRM_AMT) is null 
							then AVG(FCM.LTV_STD_CALC)
						else
						  SUM(FCM.LTV_STD_CALC * FCM.B2_IRB_EAD_POST_CRM_AMT)/SUM(FCM.B2_IRB_EAD_POST_CRM_AMT) 
					END AS LTV_STD_CALC,
					cast(FCM.POOL_GROUP_ID as decimal(22)) as POOL_GROUP_ID,
					FCM.POOL_ID,
					FCM.PRODUCT_TYPE_CODE,

					case
						when SUM(FCM.B2_IRB_EAD_POST_CRM_AMT) = 0 or SUM(FCM.B2_IRB_EAD_POST_CRM_AMT) is null then AVG(FCM.INDEXED_LTV)
								else
										  SUM(FCM.INDEXED_LTV * FCM.B2_IRB_EAD_POST_CRM_AMT)/SUM(FCM.B2_IRB_EAD_POST_CRM_AMT)
					END AS INDEXED_LTV,
					case when SUM(FCM.B2_IRB_EAD_POST_CRM_AMT) = 0 or SUM(FCM.B2_IRB_EAD_POST_CRM_AMT) is null then AVG(FCM.ORIGINAL_LTV)
											  else
										  SUM(FCM.ORIGINAL_LTV * FCM.B2_IRB_EAD_POST_CRM_AMT)/SUM(FCM.B2_IRB_EAD_POST_CRM_AMT)
					END AS ORIGINAL_LTV,
					FCM.WALKER_COST_CENTRE,
					FCM.SIC_CODE,
					SUM(FCM.SECURED_DRAWN_BAL_GBP) as SECURED_DRAWN_BAL_GBP,
					SUM(FCM.SECURED_UNDRAWN_BAL_GBP) as SECURED_UNDRAWN_BAL_GBP,
					FCM.RISK_PLATFORM_PRODUCT_TYPE,
					cast(MAX(FCM.B1_CCF) as decimal(7,4)) as B1_CCF,
					cast(MAX(FCM.B2_FIRB_CCF) as decimal(7,4)) as B2_FIRB_CCF,
					cast(MAX(FCM.B2_IRB_CCF) as decimal(9,4)) as B2_IRB_CCF,
					cast(MAX(FCM.B2_STD_CCF) as decimal(7,4)) as B2_STD_CCF,
					cast(MAX(FCM.run_id) AS int) as run_id
					FROM rtd_shared_trans.fct_capital_measures FCM
					inner join 
					(SELECT    
										   REO.POOL_ID
										  ,AVG(REO.B2_APP_ADJUSTED_PD_RATIO) AS B2_APP_ADJUSTED_PD_RATIO
										  ,AVG(REO.B2_IRB_ADJUSTED_PD_RATIO) AS B2_IRB_ADJUSTED_PD_RATIO
										  ,AVG(REO.PD_RATIO_CALC_UNADJUSTED) AS PD_RATIO_CALC_UNADJUSTED
										  ,case when SUM(REO.B2_IRB_EAD_POST_CRM_AMT) = 0 or SUM(REO.B2_IRB_EAD_POST_CRM_AMT) is null
										  then AVG(REO.B2_APP_ADJUSTED_LGD_RATIO)
											  else
										  SUM(REO.B2_APP_ADJUSTED_LGD_RATIO * REO.B2_IRB_EAD_POST_CRM_AMT)/SUM(REO.B2_IRB_EAD_POST_CRM_AMT)
										  END AS B2_APP_ADJUSTED_LGD_RATIO   
										  ,case when SUM(REO.B2_IRB_EAD_POST_CRM_AMT) = 0 or SUM(REO.B2_IRB_EAD_POST_CRM_AMT) is null
										  then AVG(REO.B2_APP_UNADJUSTED_LGD_RATIO)
											  else
										  SUM(REO.B2_APP_UNADJUSTED_LGD_RATIO * REO.B2_IRB_EAD_POST_CRM_AMT)/SUM(REO.B2_IRB_EAD_POST_CRM_AMT) 
										  END AS B2_APP_UNADJUSTED_LGD_RATIO
										  ,case when SUM(REO.B2_IRB_EAD_POST_CRM_AMT) = 0 or SUM(REO.B2_IRB_EAD_POST_CRM_AMT) is null
										  then AVG(REO.B2_IRB_ADJUSTED_LGD_RATIO)
											  else
										  SUM(REO.B2_IRB_ADJUSTED_LGD_RATIO * REO.B2_IRB_EAD_POST_CRM_AMT)/SUM(REO.B2_IRB_EAD_POST_CRM_AMT) 
										  END AS B2_IRB_ADJUSTED_LGD_RATIO
										  ,CASE when SUM(REO.B2_IRB_EAD_POST_CRM_AMT) = 0 or SUM(REO.B2_IRB_EAD_POST_CRM_AMT) is null
										  then AVG(REO.B2_IRB_UNADJUSTED_LGD_RATIO)
											  else
										  SUM(REO.B2_IRB_UNADJUSTED_LGD_RATIO * REO.B2_IRB_EAD_POST_CRM_AMT)/SUM(REO.B2_IRB_EAD_POST_CRM_AMT) 
										  END AS B2_IRB_UNADJUSTED_LGD_RATIO
								FROM rtd_shared_trans.fct_capital_measures REO
								WHERE cast(REO.DAY_RK as bigint) = {V_DAY_RK}  and 
								REO.run_id = {latest_run}
								AND REO.TYPE = 'RET' 
								 GROUP BY REO.DAY_RK,REO.src_sys_code,REO.POOL_ID ) INP
					WHERE FCM.POOL_ID = INP.POOL_ID and 
					    FCM.run_id = {latest_run}
								   AND cast(FCM.DAY_RK as bigint)= {V_DAY_RK}
								   AND FCM.src_sys_code = 'ATHENARG' 
								   AND FCM.TYPE = 'RET'
												GROUP BY 
														FCM.AS_AT_DATE
														,FCM.B2_APPROVED_APPROACH_CODE
														,FCM.COREP_PRODUCT_CATEGORY
														,FCM.DAY_RK
														,FCM.DEFAULTED_EXPOSURE_FLAG
														,FCM.ORIGINAL_CURRENCY_CODE
														,FCM.src_sys_code
														,FCM.POOLED_EXPOSURE_ID
														,FCM.COUNTRY_OF_INCORPORATION
														,FCM.POOL_GROUP_ID
														,FCM.POOL_ID
														,FCM.PRODUCT_TYPE_CODE
														,INP.B2_APP_ADJUSTED_PD_RATIO
														,INP.B2_IRB_ADJUSTED_PD_RATIO
														,INP.PD_RATIO_CALC_UNADJUSTED
														,INP.B2_APP_ADJUSTED_LGD_RATIO
														,INP.B2_APP_UNADJUSTED_LGD_RATIO
														,INP.B2_IRB_ADJUSTED_LGD_RATIO
														,INP.B2_IRB_UNADJUSTED_LGD_RATIO
														,FCM.WALKER_COST_CENTRE
														,FCM.SIC_CODE
														,FCM.RISK_PLATFORM_PRODUCT_TYPE """
	
	final_df = spark.sql(aggregation_query)
	
	end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
	
	source_cnt = spark.sql(f""" select count(*) from rtd_shared_trans.fct_capital_measures where run_id = {latest_run} """).collect()[0][0]
	target_cnt = spark.sql(f""" select count(*) from rtd_shared_trans.agg_exposure_l1 where run_id = {latest_run} """).collect()[0][0]
	
	query = f""" insert into rtd_shared_trans.dim_run select cast({latest_run} as bigint) as run_id,
						cast("consolidation" as string) as process_name,
						cast("consolidation of AIRB,B1_std_b2_std" as string) as process_desc,
						cast('{start_time}' as timestamp) as start_time,
						cast('{end_time}' as timestamp) as end_time,
						cast('{source_cnt}' as bigint) as source_count,
						cast('{target_cnt}' as bigint) as target_count,
						cast(" " as string) as jurisdiction 
		    """
						  
	output_df = spark.sql(query)
	
	dim_control_completed = f""" INSERT INTO rtd_shared_trans.dim_control 
							select 
							cast('{latest_run}' as bigint) as run_id,
							cast(to_date(from_unixtime(unix_timestamp(cast({V_DAY_RK} as string) ,'yyyyMMdd'))) as date) as business_date,
							cast('COMPLETED' as string) as status,
							cast('{start_time}' as timestamp) as start_time,
							cast('{end_time}' as timestamp) as end_time """
	
	dim_completed_df = spark.sql(dim_control_completed)
							
	
	
	
