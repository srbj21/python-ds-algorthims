from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql import Window
from pyspark.sql.functions import *
from pyspark.sql import functions
from pyspark.sql.types import *
from pyspark.sql.functions import lit
from pyspark.sql.functions import unix_timestamp
from collections import namedtuple
import pyspark.sql.functions as F
import operator
import sys
sys.path.append('/home/hadoop/rtd/Crm_Allocation')
from shared.logger.rwa_logger_helper import rwaLogger as rwa_logger, create_appname


def fun_crm_alloc(row):
	list_New = []
	flag = True
	flag_process = False
	key_Col = row.crm_exp_link_id
	list_Col = row.crm_struct_sorted_col
	crm_amt_balance = 0.0
	exp_amt_balance = 0.0
	crm_rank = 0
	exp_rank = 0
	temp_b2_std_post_deposit_net_amt = 0.0
	tmp_crm_rank = 1
	exp_amt_dict = {}
	flag_skip_message = "Skip-No"

	#Traversing List 
	for i in list_Col:
		std_effective_crm_amount = 0.0
		std_eligible_crm_amount = 0.0
		crm_calc = 9999999999

		##TODO : For Edge Case Scenario : FUTURE
		#if tmp_crm_rank != i.crm_mitigant_rank:
		#	flag = False
		#elif tmp_crm_rank == i.crm_mitigant_rank:
		#	tmp_crm_rank = i.crm_mitigant_rank
		#	if (tmp_crm_rank != 1):
		#		flag = True

		# Created Ourput Structure		
		myStruct = namedtuple("cols",["hm","hfx","volatility_haircut","crm_exp_rank","crm_mitigant_rank","exposure_rank","crm_amount_gbp","b2_std_post_deposit_net_amt","b2_std_ccf","deal_id","mitigant_id","exposure_risk_weight","guarantee_rw","std_eligible_crm_amount","std_effective_crm_amount","crm_amt_balance","exp_amt_balance","flag_skip_message","hc_value","crm_rank","exp_rank","crm_type_code"])

		#Calculating Haircut Value i.e ((1-hv-hfx)*hm*b2_std_ccf)
		hc_val = 1 - float(i.hfx) - float(i.volatility_haircut)
		hc_value = hc_val * float(i.hm) * float(i.b2_std_ccf)

		#Decicion : Whether Record to be Process or skip 
		flag_skip = True
		if flag_process:
			if ((crm_rank != i.crm_mitigant_rank) or (exp_rank != i.exposure_rank)):
				flag_skip = False
				flag_skip_message = "Skip-Yes"
			elif ((crm_rank == i.crm_mitigant_rank) and (exp_rank == i.exposure_rank)):
				flag_process = False
				flag_skip = True
				flag_skip_message = "Skip-No"

		##TODO : For Edge Case Scenario : FUTURE
		#if i.b2_std_post_deposit_net_amt <= 0:
		#	flag_skip = False
		#	flag_skip_message = "b2_std_post_deposit_net_amt is lessthan zero" 

		## CRM Allocation Process Start
		if flag_skip:

			#Capturing temp_b2_std_post_deposit_net_amt for given deal_id
			if len(exp_amt_dict) !=0 and i.deal_id in exp_amt_dict.keys():
				temp_b2_std_post_deposit_net_amt = exp_amt_dict.get(i.deal_id)
			else:
				temp_b2_std_post_deposit_net_amt = i.b2_std_post_deposit_net_amt
				exp_amt_dict[i.deal_id] = i.b2_std_post_deposit_net_amt

			if hc_value != 0:
				crm_calc = temp_b2_std_post_deposit_net_amt / hc_value
			elif (hc_value == 0 and i.b2_std_post_deposit_net_amt == 0 ):
				crm_calc = 0.0

			#Excluding Records as per requirement. Moving To Next Best CRM
			if((i.guarantee_rw > i.exposure_risk_weight) or ((i.hm is None) or (i.hm == ''))):
				flag_skip_message = "Either guarantee_rw is higher or hm value is none"
				crm_rank = i.crm_mitigant_rank + 1
				flag = True
				flag_process = True
				continue
			else:
				if flag:
					crm_amt_balance = 0.0
					exp_amt_balance = 0.0
					flag = False
					if (i.crm_amount_gbp < crm_calc):
						std_eligible_crm_amount = i.crm_amount_gbp
					else:
						std_eligible_crm_amount = crm_calc
					crm_amt_balance = i.crm_amount_gbp - std_eligible_crm_amount
					#std_eligible_crm_amount = min(i.crm_amount_gbp,crm_calc)
					std_effective_crm_amount = std_eligible_crm_amount * hc_value
					#exp_amt_balance = temp_b2_std_post_deposit_net_amt - std_eligible_crm_amount
					exp_amt_balance = temp_b2_std_post_deposit_net_amt - std_effective_crm_amount
					exp_amt_dict[i.deal_id] = exp_amt_balance

				else:
					if (crm_amt_balance < crm_calc):
						std_eligible_crm_amount = crm_amt_balance
					else:
						std_eligible_crm_amount = crm_calc
					#std_eligible_crm_amount = min(i.crm_amt_balance,crm_calc)
					std_effective_crm_amount = std_eligible_crm_amount * hc_value
					crm_amt_balance = crm_amt_balance - std_eligible_crm_amount
					#exp_amt_balance = exp_amt_dict.get(i.deal_id) - std_eligible_crm_amount
					exp_amt_balance = temp_b2_std_post_deposit_net_amt - std_effective_crm_amount
					exp_amt_dict[i.deal_id] = exp_amt_balance

				#Decicion :  Required Parameters are setting for Next Record.
				if crm_amt_balance <= 0 and exp_amt_balance <= 0:
					flag = True
					flag_process = True
					crm_rank = i.crm_mitigant_rank + 1
					exp_rank = i.exposure_rank + 1
				if crm_amt_balance <= 0 and exp_amt_balance > 0:
					flag = True
					flag_process = True
					crm_rank = i.crm_mitigant_rank + 1
					exp_rank = i.exposure_rank
				if crm_amt_balance > 0 and exp_amt_balance <= 0:
					flag_process = True
					crm_rank = i.crm_mitigant_rank
					exp_rank = i.exposure_rank + 1

			#Writing Records to the List
			crm_amount_alloc = myStruct(i.hm,i.hfx,i.volatility_haircut,i.crm_exp_rank,i.crm_mitigant_rank,i.exposure_rank,i.crm_amount_gbp,i.b2_std_post_deposit_net_amt,i.b2_std_ccf,i.deal_id,i.mitigant_id,i.exposure_risk_weight,i.guarantee_rw,std_eligible_crm_amount,std_effective_crm_amount,crm_amt_balance,exp_amt_balance,flag_skip_message,hc_value,crm_rank,exp_rank,i.crm_type_code)
			list_New.append(crm_amount_alloc)

	return(key_Col,list_New)

def get_param(param_name):
	"""
	Gets value of passed parameter_name from parameters table.
	"""
	df = spark.sql(f"""SELECT * FROM {PARAM_TABLE}""")
	param_df = df.filter(col("param_name") == param_name)
	param_list = param_df.collect()
	if param_list:
		return param_list[0].asDict()['param_value']
	else:
		rwa_logger("rwa-WAR-006", comment = f"Business date for {param_name} process doesn't exist")
		raise Exception("Business date for this process doesn't exist")

if __name__ == "__main__":

	spark = SparkSession.builder.master("yarn").enableHiveSupport().getOrCreate()
	sc = spark.sparkContext
	spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
	spark.sparkContext.setLogLevel("ERROR")
	PARAM_TABLE="RTD_SHARED_TRANS.ETL_PARAMETERS"
	attributes = dict()

	try:
		control_db = get_param("REF_DB")
		calc_input_db = get_param("REF_DB")
		calc_output_db = get_param("REF_DB")
		reference_db = get_param("REF_DB")
		run_id = 569
		REPORTING_DATE = get_param('REPORTING_DATE')

	except Exception as msg:
		print(msg)
		rwa_logger("rwa-ERR-112", comment = f"Error {msg} occured while running CRM ALLOCATION STD")
	try:
		print()
		print("=========================")
		print("CRM_Guarantee_RW  STARTED")
		print("=========================")

		CRM_Priority_query = """
					SELECT
					CRM.Mitigant_ID,
					CRM.CRM_EXP_Link_ID,
					CRM.CRM_Type_Code,
					CRM.STD_Risk_Class_of_LE,
					CRM.credit_quality_step,
					CRM.sovereign_le_cqs,
					CRM.country_currency_code,
					CRM.sovereign_equivalent_flag,
					CRM.EEA_equivalent_flag,
					CRM.country_currency_match_flag,
					CRM.EU_currency_flag,
					CRM.EU_counterparty_flag,
					CRM.Mitigant_original_Maturity,
					CRM.Mitigant_Currency,
					CRM.Mitigant_Residual_Maturity,
					MIN(REF.priority) AS MIN_priority
					FROM  rtd_shared_trans.CRM_sample CRM
					LEFT OUTER JOIN rtd_shared_trans.CRM_STD_GTEE_RW REF
					ON (
					CRM.STD_Risk_Class_of_LE = REF.STD_Risk_Class_of_LE 
					AND (CASE WHEN REF.credit_quality_step = 'N/A' THEN CRM.credit_quality_step ELSE REF.credit_quality_step END = CRM.credit_quality_step)
					AND (CASE WHEN REF.sovereign_le_cqs = 'N/A' THEN CRM.sovereign_le_cqs ELSE REF.sovereign_le_cqs END = CRM.sovereign_le_cqs)
					AND (CASE WHEN REF.country_currency_match_flag = 'N/A' THEN CRM.country_currency_match_flag ELSE REF.country_currency_match_flag END = CRM.country_currency_match_flag)
					AND (CASE WHEN REF.sovereign_equivalent_flag = 'N/A' THEN CRM.sovereign_equivalent_flag ELSE REF.sovereign_equivalent_flag END = CRM.sovereign_equivalent_flag)
					AND (CASE WHEN REF.EEA_equivalent_flag = 'N/A' THEN CRM.EEA_equivalent_flag ELSE REF.EEA_equivalent_flag END = CRM.EEA_equivalent_flag)
					AND (CASE WHEN REF.EU_currency_flag = 'N/A' THEN CRM.EU_currency_flag ELSE REF.EU_currency_flag END = CRM.EU_currency_flag)
					AND (CASE WHEN REF.EU_counterparty_flag = 'N/A' THEN CRM.EU_counterparty_flag ELSE REF.EU_counterparty_flag END = CRM.EU_counterparty_flag)
					AND CRM.Mitigant_original_Maturity >= REF.Original_Maturity_start_from  AND CRM.Mitigant_original_Maturity < REF.original_Maturity_end_to
					AND CRM.Mitigant_Residual_Maturity >= REF.Mitigant_Residual_Maturity_start_from AND CRM.Mitigant_Residual_Maturity < REF.Mitigant_Residual_Maturity_end_to
					) 
					GROUP BY CRM.Mitigant_ID,CRM.CRM_EXP_Link_ID,CRM.CRM_Type_Code,CRM.STD_Risk_Class_of_LE,CRM.credit_quality_step,
					CRM.sovereign_le_cqs,CRM.country_currency_code,CRM.sovereign_equivalent_flag,CRM.EEA_equivalent_flag,CRM.country_currency_match_flag,
					CRM.EU_currency_flag,CRM.EU_counterparty_flag,CRM.Mitigant_original_Maturity,CRM.Mitigant_Currency,CRM.Mitigant_Residual_Maturity
					"""

		CRM_Priority = spark.sql(CRM_Priority_query)
		CRM_Priority.show(10)
		CRM_Priority.createOrReplaceTempView("CRM_Priority")

		CRM_Guarantee_RW = """
				SELECT
				CRM_P.Mitigant_ID,
				CRM_P.CRM_EXP_Link_ID,
				CRM_P.CRM_Type_Code,
				CRM_P.STD_Risk_Class_of_LE,
				CRM_P.credit_quality_step,
				CRM_P.sovereign_le_cqs,
				CRM_P.country_currency_code,
				CRM_P.sovereign_equivalent_flag,
				CRM_P.EEA_equivalent_flag,
				CRM_P.country_currency_match_flag,
				CRM_P.EU_currency_flag,
				CRM_P.EU_counterparty_flag,
				CRM_P.Mitigant_original_Maturity,
				CRM_P.Mitigant_Currency,
				CRM_P.Mitigant_Residual_Maturity,
				CASE WHEN CRM_P.STD_Risk_Class_of_LE  NOT IN ('Administrative bodies and non-commercial undertakings', 'Bank', 'Corporate', 'International organisations',
				'Regional governments or local authorities', 'Sovereign', 'Sovereign OR Regional governments or local authorities') THEN 78 
				ELSE CRM_P.MIN_priority 
				END AS MIN_priority,
				CASE WHEN CRM_P.CRM_TYPE_CODE = 'C' THEN 0.0 
				     WHEN CRM_P.STD_Risk_Class_of_LE  NOT IN ('Administrative bodies and non-commercial undertakings', 'Bank', 'Corporate', 'International organisations',
				'Regional governments or local authorities', 'Sovereign', 'Sovereign OR Regional governments or local authorities') THEN 12.5
				ELSE GTEE_REF.risk_weight
				END AS Guarantee_RW
				FROM  CRM_Priority CRM_P
				LEFT OUTER JOIN rtd_shared_trans.CRM_STD_GTEE_RW GTEE_REF
				ON (CRM_P.MIN_priority = GTEE_REF.priority)
				"""

		CRM_Guarantee_RW_output = spark.sql(CRM_Guarantee_RW)
		CRM_Guarantee_RW_output.show(5)
		CRM_Guarantee_RW_output.createOrReplaceTempView("CRM_Guarantee_RW")
		print()
		print("=============================")
		print("Exposure_Ranking STARTED")
		print("=============================")

		Exposure_Ranking = """
				SELECT 
				deal_id,
				CRM_EXP_LINK_ID,
				b2_std_post_deposit_net_amt	,
				b2_std_ccf,
				exposure_currency,
				exposure_residual_maturity,
				exposure_risk_weight ,
				exposure_classification,
				RANK () OVER (PARTITION BY CRM_EXP_LINK_ID 
						 ORDER BY
				             CASE
				                WHEN (exposure_classification not in('Letter of Credit','Off Balance'))
				                THEN 1
				                WHEN (exposure_classification = 'Letter of Credit')
				                THEN 2
				                ELSE 3
				             END ASC,
				             exposure_risk_weight DESC,     
				             exposure_residual_maturity DESC,
				             DEAL_ID ASC)
				        Exposure_Rank	
				FROM rtd_shared_trans.exp_sample
				"""

		Exposure_Ranking_output = spark.sql(Exposure_Ranking)
		Exposure_Ranking_output.show(5)
		Exposure_Ranking_output.createOrReplaceTempView("CRM_Exposure_Ranking")
		print()
		print("==========================================")
		print("Exposure_Ranking IS COMPLETED SUCCESSFULLY")
		print("==========================================")

		print()
		print("=========================")
		print("CRM_HV  STARTED")
		print("=========================")

		CRM_HV = """
				SELECT 
				CRM.mitigant_id,
				CRM.crm_exp_link_id,
				CRM.crm_type_code,
				CRM.crm_sub_type_code,
				CRM.mitigant_residual_maturity,
				CRM.std_risk_class_of_le,
				CRM.credit_quality_step_of_issue,
				CRM.Sovereign_Equivalent_Flag,
				CRM.volatility_haircut_source,
				CASE
				    WHEN CRM.CRM_TYPE_CODE IN ('GU','CD') THEN 0
				    wHEN REF.HV IS NOT NULL THEN ref.hv
				    WHEN CRM.volatility_haircut_source IS NOT NULL THEN CRM.volatility_haircut_source
				    ELSE NULL
				END AS volatility_haircut
				FROM rtd_shared_trans.crm_sample CRM 
				LEFT OUTER JOIN rtd_shared_trans.CRM_HV_Reference_Data REF
				ON (CRM.crm_sub_type_code = REF.Collateral_Type
				AND (CASE
				    WHEN REF.std_exposure_class_of_issue = 'N/A' THEN CRM.std_risk_class_of_le 
					ELSE REF.std_exposure_class_of_issue END = CRM.std_risk_class_of_le)
				AND (CASE
				    WHEN REF.Credit_Quality_Step_of_Issue = 'N/A' THEN CRM.credit_quality_step_of_issue 
					ELSE REF.Credit_Quality_Step_of_Issue END = CRM.credit_quality_step_of_issue) 
				AND (CASE
				    WHEN REF.Sovereign_Equivalent_Flag = 'N/A' THEN CRM.Sovereign_Equivalent_Flag 
					ELSE REF.Sovereign_Equivalent_Flag END = CRM.Sovereign_Equivalent_Flag)  
				AND CRM.mitigant_residual_maturity > (CASE
				    WHEN ref.Residual_Maturity_start_from IS NULL THEN (CRM.mitigant_residual_maturity - 1)
					ELSE ref.Residual_Maturity_start_from END)
				AND CRM.mitigant_residual_maturity <= (CASE
				    WHEN ref.Residual_Maturity_end_to IS NULL THEN CRM.mitigant_residual_maturity 
					ELSE ref.Residual_Maturity_end_to END))
				"""

		CRM_HV_output = spark.sql(CRM_HV)
		CRM_HV_output.show(5)
		CRM_HV_output.createOrReplaceTempView("CRM_HV")

		print()
		print("==========================================")
		print("CRM_HV IS COMPLETED SUCCESSFULLY")
		print("==========================================")

		print()
		print("=============================")
		print("CRM_Eligibility_Check STARTED")
		print("=============================")

		CRM_Eligibility_Check = """
				SELECT 
				CRM.MITIGANT_ID,
				HV.CRM_EXP_LINK_ID,
				CRM.CRM_TYPE_CODE,
				CRM.CRM_SUB_TYPE_CODE,
				CRM.Mitigant_Residual_Maturity,
				HV.volatility_haircut, 
				CASE	
					 WHEN (CRM.CRM_SUB_TYPE_CODE <> 'PG' AND CRM.Mitigant_Residual_Maturity > 0 AND HV.volatility_haircut IS NOT NULL) THEN 'Y'
					 ELSE 'N'
				END as B2_STD_CRM_Eligibility_flag
				FROM rtd_shared_trans.CRM_SAMPLE CRM
				INNER JOIN rtd_shared_trans.CRM_HV HV
				ON CRM.MITIGANT_ID = HV.MITIGANT_ID
				"""

		CRM_Eligibility_Check_output = spark.sql(CRM_Eligibility_Check)
		CRM_Eligibility_Check_output.show(5)
		CRM_Eligibility_Check_output.createOrReplaceTempView("CRM_Eligibility_Check")

		print()
		print("===============================================")
		print("CRM_Eligibility_Check IS COMPLETED SUCCESSFULLY")
		print("===============================================")

		print()
		print("=============================")
		print("CRM_Ranking STARTED")
		print("=============================")

		CRM_Ranking = """
				SELECT
				CRM.Mitigant_Id,
				CRM.CRM_EXP_Link_ID,
				CRM.CRM_Type_Code,
				CHK.B2_STD_CRM_Eligibility_flag,
				CRM.STD_Risk_Class_of_LE,
				CRM.credit_quality_step,
				CRM.sovereign_le_cqs,
				CRM.Mitigant_original_Maturity,
				CRM.crm_amount_gbp,
				CRM.Mitigant_Currency,
				CRM.Mitigant_Residual_Maturity,
				GRT.Guarantee_RW,
				HV.volatility_haircut,
				RANK() OVER (PARTITION BY CRM.CRM_EXP_Link_ID 
				                 ORDER BY  
				                          CASE 
				                              WHEN CRM.CRM_TYPE_CODE = 'C' THEN 1
											  WHEN CRM.CRM_TYPE_CODE = 'GU' THEN 2
											  WHEN CRM.CRM_TYPE_CODE = 'CD' THEN 3
											  ELSE 4
										  END ASC,	
										  HV.Volatility_Haircut ASC,
				                          GRT.Guarantee_RW ASC,   
										  CRM.Mitigant_Residual_Maturity DESC,
										  CRM.Mitigant_Id ASC
							) CRM_Mitigant_Rank
				FROM rtd_shared_trans.CRM_sample CRM  
				LEFT OUTER JOIN rtd_shared_trans.CRM_Guarantee_RW GRT  ON  (CRM.Mitigant_Id = GRT.Mitigant_Id)
				LEFT OUTER JOIN rtd_shared_trans.CRM_hv HV  ON  (CRM.Mitigant_Id = HV.Mitigant_Id)
				LEFT OUTER JOIN rtd_shared_trans.CRM_Eligibility_Check CHK  ON  (HV.Mitigant_Id = CHK.mitigant_id)
				WHERE CHK.B2_STD_CRM_Eligibility_flag = 'Y'
				"""

		CRM_Ranking_output = spark.sql(CRM_Ranking)
		CRM_Ranking_output.show(5)
		CRM_Ranking_output.createOrReplaceTempView("CRM_Ranking")
		spark.catalog.dropTempView("CRM_Guarantee_RW")
		spark.catalog.dropTempView("CRM_HV")
		spark.catalog.dropTempView("CRM_Eligibility_Check")



		print()
		print("===============================================")
		print("CRM_Ranking IS COMPLETED SUCCESSFULLY")
		print("===============================================")

		hm_hfx_hql = """
		SELECT
			 CRM.crm_type_code,
		 EXPO.crm_exp_link_id,
		 EXPO.exposure_residual_maturity	,
		 EXPO.b2_std_post_deposit_net_amt,
		 EXPO.b2_std_ccf,
		 EXPO.exposure_risk_weight,
		 cast(EXPO.exposure_rank as int),
		 EXPO.deal_id,
		 CRM.mitigant_id, 
		 CRM.volatility_haircut/100  as volatility_haircut,
		 CRM.mitigant_residual_maturity,
		 CRM.mitigant_original_maturity,
		 CRM.guarantee_rw,
		 CRM.crm_amount_gbp ,
		 cast(CRM.crm_mitigant_rank as int),
		 cast(concat(CRM.crm_mitigant_rank,EXPO.exposure_rank) as int) as crm_exp_rank,
		 CASE
			WHEN CRM.mitigant_residual_maturity < EXPO.exposure_residual_maturity THEN
				CASE
					WHEN CRM.mitigant_original_maturity > 1 AND CRM.mitigant_residual_maturity > 3 / 12
					THEN (LEAST(CRM.mitigant_residual_maturity,LEAST(EXPO.exposure_residual_maturity,5))-0.25)/(LEAST(EXPO.exposure_residual_maturity,5)-0.25)
					ELSE NULL
				END
			ELSE 1
			END as hm,
		  CASE	
			WHEN exposure_currency  = mitigant_currency THEN 0
			ELSE 0.08
		  END as hfx
		FROM CRM_Ranking CRM
		INNER JOIN CRM_Exposure_Ranking EXPO
		ON CRM.crm_exp_link_id = EXPO.crm_exp_link_id and  CRM.guarantee_rw is not null"""
		hm_hfx_df = spark.sql(hm_hfx_hql)
		spark.catalog.dropTempView("CRM_Exposure_Ranking")
		spark.catalog.dropTempView("CRM_Ranking")

		## Created Strct to maintain column reference
		crm_amount_concat_df  = hm_hfx_df.select("crm_exp_link_id",F.struct("crm_exp_rank","crm_mitigant_rank","exposure_rank","hm","hfx","volatility_haircut","crm_amount_gbp","b2_std_post_deposit_net_amt","b2_std_ccf","deal_id","mitigant_id","exposure_risk_weight","guarantee_rw","crm_type_code").alias("crm_struct_col"))

		## All key records are added to the list
		crm_amount_list_df = crm_amount_concat_df.groupBy("crm_exp_link_id").agg(F.collect_list("crm_struct_col").alias("crm_list_col"))


		## Sorting List with key order "crm_mitigant_rank","exposure_rank"
		crm_amount_sorted_df  = crm_amount_list_df.rdd.map(lambda x: [x[0], sorted(x[1],key=operator.itemgetter(1,2))]).toDF(["crm_exp_link_id","crm_struct_sorted_col"])

		## Functio Called for CRM ALLOCATION Process
		crm_amount_rdd = crm_amount_sorted_df.rdd.map(lambda x: fun_crm_alloc(x) )

		##Converting RDD to dataframe
		crm_amount_df = crm_amount_rdd.toDF(["crm_exp_link_id","listCol"])

		##Exploding List with keycolumn
		crm_amount_explode_df = crm_amount_df.select("crm_exp_link_id",F.explode("listCol").alias("newCol"))
		crm_amount_alloc_df = crm_amount_explode_df.select("crm_exp_link_id",F.col("newCol.*"))

		## Exclude the records in final outpur
		df_final = crm_amount_alloc_df.filter(F.col("b2_std_post_deposit_net_amt") != 0)
		output_table="rtd_shared_trans.rwa_engine_crm_output_std"
		#df_final_1 = df_final.withColumn("run_id",lit(run_id)).withColumn("STD_EFF_COLL_AMT_POST_HCUT ",when( df_final.crm_type_code == 'C',df_final.std_effective_crm_amount).otherwise(lit(None))).withColumn("STD_EFF_GTEE_AMT",when( (df_final.crm_type_code == "GU" | df_final.crm_type_code == "CD"),df_final.std_effective_crm_amount).otherwise(lit(None)))
		df_final_2 = df_final.withColumn("run_rk",lit(run_id)).withColumn("std_eff_coll_amt_post_hcut",when( df_final.crm_type_code == 'C',df_final.std_effective_crm_amount).otherwise('')).withColumn("std_eff_gtee_amt",expr("CASE WHEN (crm_type_code == 'GU' OR crm_type_code == 'CD') THEN Std_effective_crm_amount ELSE '' END")).withColumnRenamed("hm","maturity_mismatch_adjustment_factor").withColumnRenamed("hfx","fx_haircut").withColumn("std_elig_coll_amt",when( df_final.crm_type_code == 'C',df_final.std_eligible_crm_amount).otherwise('')).withColumn("std_elig_gtee_amt",expr("CASE WHEN (crm_type_code == 'GU' OR crm_type_code == 'CD') THEN Std_eligible_crm_amount ELSE '' END"))
		df_final_1 = df_final_2.withColumn("std_gtee_cds_rwa",expr("CASE WHEN (crm_type_code == 'GU' OR crm_type_code == 'CD') THEN NVL(std_eff_gtee_amt,0)*NVL(guarantee_rw,0) ELSE '' END")).withColumn("day_rk",to_date(concat(substring(lit(REPORTING_DATE),1,4),lit('-'),substring(lit(REPORTING_DATE),4,2),lit('-'),substring(lit(REPORTING_DATE),6,2))))
		df_source = spark.sql("select * from rtd_shared_trans.rwa_engine_crm_output_std limit 1")
		df_final_1.printSchema()
		df_final_1.show()
		df_final_formatted = df_final_1.select([F.col(i).alias(i.lower()) for i in df_source.columns])
		df_final_formatted.coalesce(1).write.mode('overwrite').insertInto(output_table)
		#df_final.coalesce(1).write.mode('overwrite').parquet("s3://bucket-eu-west-1-897144472116-processed-data/transformation/rtd/rtd_shared_trans/crm_alloc_new")
		print("crm_amount_calculation_Completed")
	except MemoryError:
		print("Memory issue encountered")
		rwa_logger("rwa-ERR-112", comment = f"Error occured while running CRM ALLOCATION STD")
		#break;
	except Exception as e:
		print("Error while inserting data")
		rwa_logger("rwa-ERR-112", comment = f"Error occured while running CRM ALLOCATION STD")
		raise e
	else:
		print("CRM ALLOCATION STD Allocation Completed")
		#fetch the final count of records
		#feed_dim_control(attributes)
