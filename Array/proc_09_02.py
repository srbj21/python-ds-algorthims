from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import expr,col
from pyspark.sql.functions import lit

db='rtd_mart'

tbl_dim_year_month='dim_year_month'
tbl_dim_dpm_return='dim_dpm_return'
tbl_dim_pd_band='dim_pd_band'
tbl_dim_division='dim_division'
tbl_dim_country_enriched='dim_country_enriched'
tbl_dim_asset_class='dim_asset_class'
tbl_agg_corep_crd4_l1='agg_corep_crd4_l1'
tbl_dim_rep_intragroup_map='dim_rep_intragroup_map'
tbl_dpm_parameter='dpm_parameter'

def proc_dpm_corep_09_02(p_reporting_date=None, p_run_rk=-1, p_return_rk=-1):
    if p_reporting_date is None:
        sysdate = datetime.now().strftime('%Y-%m-%d')
        p_at_current_month='N'
        v_reporting_date=get_reporting_day(sysdate,p_at_current_month)
        print(v_reporting_date)
    else:
        # format for v_reporting_date is '20211130', 'YYYYMMDD'
        v_reporting_date=p_reporting_date
    
    v_report_date_str=str(v_reporting_date)
    v_year_month_rk = v_report_date_str[0:6]
    print(v_year_month_rk)
    
    v_return_code=get_return_code(p_return_rk)
    
    v_ytd = get_year(v_year_month_rk)
    
    df_trs_corep_09_02=create_trs_corep_09_02_stg(v_year_month_rk,p_return_rk,v_ytd,p_run_rk)
    #df_trs_corep_09_02.show()
    
    
    # change -2 to -1 in add_months before moving it to prod as we need to test this code for Nov'21 and we are running this code in Jan'22 

def get_reporting_day(sysdate,p_at_current_month):
    query=f'''select reporting_day_rk from {db}.{tbl_dim_year_month} where year_month_rk= (case when '{p_at_current_month}' ='Y' then 
    date_format(to_date(from_unixtime(unix_timestamp('{sysdate}',"yyyy-MM-dd"))),"YYYYMM")  when '{p_at_current_month}' ='N' 
    then date_format(add_months(to_date(from_unixtime(unix_timestamp('{sysdate}',"yyyy-MM-dd"))),-2),"YYYYMM") end)'''
    
    reporting_day_rk = spark.sql(query).collect()[0][0]
    return reporting_day_rk
  
def get_return_code(p_return_rk):
    query=f'''select return_code from {db}.{tbl_dim_dpm_return} where return_rk={p_return_rk}'''
    return_code=spark.sql(query).collect()[0][0]
    return return_code

def get_year(v_year_month_rk):
    query=f'''select year from {db}.{tbl_dim_year_month} where year_month_rk={v_year_month_rk}'''
    yr= spark.sql(query).collect()[0][0]
    return yr

def create_trs_corep_09_02_stg(v_year_month_rk,p_return_rk,v_ytd,p_run_rk):
    
    df_temp_table_09_02=create_temp_trs_corep_09_02(v_year_month_rk,p_return_rk,v_ytd)
    df_temp_table_trs_09_02=df_temp_table_09_02.coalesce(10).persist()
    df_temp_table_trs_09_02.first()
    
    df_temp_table_trs_09_02.createOrReplaceTempView("temp_table_trs_09_02")
    
    v_exp_class1 = None
    v_exp_class2 = None
    v_exp_class3 = None
    v_exp_class4 = None
    v_exp_class5 = None

    
    for i in range(1,17):
        
        if (i ==1):
            v_row_id='R010'
            v_exp_class1 = 'Central governments and central banks'
            print('in if condition')
            df_1st_insrt_bef_union=run_trs_corep_09_02_stg_1(v_year_month_rk,p_return_rk,v_row_id,v_exp_class1,v_exp_class2,v_exp_class3,v_exp_class4,v_exp_class5)
            #df_first_union.show()
            df_1st_insrt_bef_union.createOrReplaceTempView('corep_stg')
            df_1st_insrt_aft_union=run_trs_corep_09_02_stg_2(v_year_month_rk,p_return_rk,v_ytd,v_row_id,v_exp_class1,v_exp_class2,v_exp_class3,v_exp_class4,v_exp_class5)
            print("in second union")
            #df_second_union.show(truncate=False)
            df_first_insert_rowid10=df_1st_insrt_bef_union.union(df_1st_insrt_aft_union)
            
            print("starting stack")
            #need to cast NULL as double to make all column datatype -same
            
            unpivot_df=unpivot_trs_corep_09_02(df_first_insert_rowid10)
            #unpivot_df.show()
            
            df_2nd_insrt_bef_union= run_trs_corep_09_02_stg_3(v_year_month_rk,p_return_rk,v_row_id,v_exp_class1,v_exp_class2,v_exp_class3,v_exp_class4,v_exp_class5)
            df_2nd_insrt_bef_union.show()
            
        elif (i ==2):
            v_row_id = 'R020'
            v_exp_class1 = 'Institutions'
            
        
        elif (i==3):
            v_row_id = 'R030'
            v_exp_class1 = 'Corporates - SME'
            v_exp_class2 = 'Corporates - Specialised Lending'
            v_exp_class3 = 'Corporates - Other'
            
        
        elif (i==4):
            v_row_id = 'R042'
            v_exp_class1 = 'Corporates - Specialised Lending'
        
        elif(i==5):
            v_row_id = 'R045'
            v_exp_class1 = 'Corporates - Specialised Lending'
            
        elif(i==6):
            v_row_id = 'R050'
            v_exp_class1 = 'Corporates - SME'
        
        elif(i==7):
            v_row_id = 'R060'
            v_exp_class1 = 'Retail - Secured by immovable property SME'
            v_exp_class2 = 'Retail - Secured by immovable property non-SME'
            v_exp_class3 = 'Retail - Qualifying revolving'
            v_exp_class4 = 'Retail - Other SME'
            v_exp_class5 = 'Retail - Other non-SME'
            
        
        elif(i==8):
            v_row_id = 'R070'
            v_exp_class1 = 'Retail - Secured by immovable property SME'
            v_exp_class2 = 'Retail - Secured by immovable property non-SME'
                
        elif(i==9):
            v_row_id = 'R080'
            v_exp_class1 = 'Retail - Secured by immovable property SME'
        
        elif (i==10):
            v_row_id = 'R090'
            v_exp_class1 = 'Retail - Secured by immovable property non-SME'
        
        elif (i==11):
            v_row_id = 'R100'
            v_exp_class1 = 'Retail - Qualifying revolving'
        
        elif (i==12):
            v_row_id = 'R110'
            v_exp_class1 = 'Retail - Other SME'
            v_exp_class2 = 'Retail - Other non-SME'
        
        elif(i==13):
            v_row_id = 'R120'
            v_exp_class1 = 'Retail - Other SME'
        
        elif(i==14):
            v_row_id = 'R130'
            v_exp_class1 = 'Retail - Other non-SME'
        
        elif(i==15):
            v_row_id = 'R140'
            v_exp_class1 = 'Equities'
        
        elif(i==16):
            v_row_id = 'R150'



    
    #before union query df except loop fields.
    #df_trs_corep_stg_1=run_trs_corep_09_02_stg_1(v_year_month_rk,p_return_rk)
    
    #after union query df except loop fields
    #df_trs_corep_stg_2=run_trs_corep_09_02_stg_2(v_year_month_rk,p_return_rk,v_ytd)
    
    #df_trs_corep_stg_2.show()
    partitions_temp_tbl=df_temp_table_trs_09_02.rdd.getNumPartitions()
    print(f"total number of partitions: {partitions_temp_tbl}")
    #partitions_sec_union =df_second_union.rdd.getNumPartitions()
    #print(f"total number of partitions in second union: {partitions_sec_union}")
    
 #one option is while creating table 'TRS_COREP_C9_02_STG' do join of all the source tables and select all the columns which 
#are required for aggregation, filter condition and from this create a dataframe and persist that dataframe and then create
#a temporary table on top of it and persist will help us in loop and we don't need to create df in every loop condition.

def create_temp_trs_corep_09_02(v_year_month_rk,p_return_rk,v_ytd):
    query=f'''select
    {v_year_month_rk},
    {p_return_rk},
    FCT.B2_IRB_ORIG_EXP_PRE_CON_FACTOR,
    DPB.MGS,
    FCT.OBSRVD_NEW_DEFAULT_EXP_AMT,
    FCT.B2_IRB_PROVISIONS_AMT,
    FCT.IMP_WRITE_OFF_AMT,
    FCT.OBSRVD_NEW_DEFAULT_CR_ADJ,
    FCT.EAD_PD_NUMERATOR,
    FCT.EAD_POST_CRM_AMT,
    FCT.EAD_LGD_NUMERATOR,
    FCT.RWA_POST_CRM_PRE_SPRT_AMOUNT,
    FCT.RWA_POST_CRM_AMOUNT,
    FCT.B2_APP_EXPECTED_LOSS_AMT,
    FCT.REPORTING_REGULATOR_CODE,
    IG.RISK_TAKER_GROUP,
    DC.COREP_ISO_CODE,
    DAC.REG_EXPOSURE_TYPE,
    FCT.RWA_CALC_METHOD,
    FCT.YEAR_MONTH_RK
    FROM  
    {db}.{tbl_dim_pd_band} DPB,
    {db}.{tbl_dim_division} DIV,
    {db}.{tbl_dim_country_enriched} DC,
    {db}.{tbl_dim_asset_class} DAC,
    {db}.{tbl_agg_corep_crd4_l1} FCT,
    {db}.{tbl_dim_rep_intragroup_map} IG,
    {db}.{tbl_dpm_parameter} d_parm_div
    WHERE
    FCT.DIVISION_RK = DIV.DIVISION_RK
    AND FCT.INCORP_COUNTRY_RK = DC.COUNTRY_RK
    AND FCT.MGS_PD_BAND_CODE_RK = DPB.PD_BAND_RK
    AND FCT.INTERNAL_TRANSACTION_FLAG = IG.INTERNAL_TRANSACTION_FLAG
    AND DAC.ASSET_CLASS_RK = FCT.COREP_IRB_ASSET_CLASS_RK
    AND IG.REPORTING_DESC IN ('External')
    AND IG.RISK_TAKER_GROUP IS NOT NULL
    AND FCT.RWA_APPROACH_CODE = 'AIRB'
    AND FCT.ADJUST_FLAG = 'A'
    AND (FCT.CRD4_REP_IRB_FLAG IN ('Y')
    OR (FCT.ACLM_PRODUCT_TYPE IN ('BASEL-OTC')
    AND FCT.BASEL_DATA_FLAG = 'N')
    OR FCT.CRD4_REP_IRBEQ_FLAG IN ('Y'))
    AND FCT.RWA_Calc_Method NOT IN ('AIRB - Equity Simple Risk Weight')
    AND FCT.YEAR_MONTH_RK IN (SELECT YEAR_MONTH_RK FROM {db}.{tbl_dim_year_month} WHERE YEAR = {v_ytd} AND YEAR_MONTH_RK <= {v_year_month_rk})
    AND NVL (FCT.REPORTING_TYPE_CODE, 'X') NOT IN ('MR', 'NC', 'OR', 'SR')
    AND fct.RRU_RK = ig.RISK_TAKER_RRU_RK
    AND fct.RISK_ON_RRU_RK = ig.RISK_ON_RRU_RK
    AND fct.YEAR_MONTH_RK = ig.YEAR_MONTH_RK
    AND ig.RISK_TAKER_LL_RRU NOT LIKE '%DECON%'
    AND DIV.DIVISION = d_parm_div.PARAMETER1_VALUE
    AND d_parm_div.param_code = 'DIVISION'
    AND d_parm_div.PARAMETER2_VALUE = 'Y'
    AND d_parm_div.valid_from_date <= "2021-11-30"
    AND d_parm_div.valid_to_date > "2021-11-30" '''
    
    df_temp_table_trs_09_02=spark.sql(query)
    
    
    return df_temp_table_trs_09_02
  
def run_trs_corep_09_02_stg_1(v_year_month_rk,p_return_rk,v_row_id,v_exp_class1,v_exp_class2,v_exp_class3,v_exp_class4,v_exp_class5):
    query=f'''SELECT 
    {v_year_month_rk} YEAR_MONTH_RK,
    {p_return_rk} RETURN_RK,
    "{v_row_id}" row_id,
    cast(SUM(B2_IRB_ORIG_EXP_PRE_CON_FACTOR) as double) AS C010,
    SUM (CASE WHEN MGS = 27 THEN B2_IRB_ORIG_EXP_PRE_CON_FACTOR END) AS C030,
    SUM (OBSRVD_NEW_DEFAULT_EXP_AMT) AS C040,
    cast(NULL as double) AS C050,
    SUM (B2_IRB_PROVISIONS_AMT) AS C055,
    SUM (IMP_WRITE_OFF_AMT) AS C060,
    SUM (OBSRVD_NEW_DEFAULT_CR_ADJ) AS c070,
    SUM (EAD_PD_NUMERATOR )/NULLIF(SUM(EAD_POST_CRM_AMT),0) as C080,
    SUM(EAD_LGD_NUMERATOR)/NULLIF(SUM(EAD_POST_CRM_AMT),0)  as C090,
    SUM ( CASE WHEN MGS = 27 THEN EAD_LGD_NUMERATOR END)
    /NULLIF(SUM( CASE WHEN MGS = 27 THEN EAD_POST_CRM_AMT END),0) as C100,
    SUM (EAD_POST_CRM_AMT) AS C105,
    SUM (RWA_POST_CRM_PRE_SPRT_AMOUNT ) AS C110,
    SUM (CASE WHEN MGS = 27 THEN RWA_POST_CRM_PRE_SPRT_AMOUNT END) AS C120,
    SUM (RWA_POST_CRM_AMOUNT) AS C125,
    SUM (B2_APP_EXPECTED_LOSS_AMT) AS c130,
    REPORTING_REGULATOR_CODE AS REPORTING_REGULATOR_CODE,
    RISK_TAKER_GROUP AS RISK_TAKER_GROUP,
    NULL AS  FOR_ADJUST_FLAG,
    CASE WHEN COREP_ISO_CODE = 'CK' AND REPORTING_REGULATOR_CODE = 'CBI' THEN 'OC' ELSE COREP_ISO_CODE END AS ISO_CODE
    FROM  
    temp_table_trs_09_02
    WHERE       
    YEAR_MONTH_RK = {v_year_month_rk}
    AND REG_EXPOSURE_TYPE IN(nvl("{v_exp_class1}",REG_EXPOSURE_TYPE),"{v_exp_class2}","{v_exp_class3}","{v_exp_class4}","{v_exp_class5}")
    AND (("{v_row_id}" = 'R140' AND UPPER (RWA_CALC_METHOD) = UPPER ('AIRB - PD/LGD Approach')) 
    OR ("{v_row_id}" = 'R045' AND UPPER (RWA_CALC_METHOD) = UPPER ('AIRB - SL Slotting Approach'))
    OR ("{v_row_id}" IN ('R030','R150') AND ((REG_EXPOSURE_TYPE = 'Corporates - Specialised Lending' 
    AND UPPER (RWA_CALC_METHOD) = UPPER ('AIRB - SL Slotting Approach'))
    OR (UPPER (NVL (RWA_CALC_METHOD, 'x')) <> UPPER ('AIRB - SL Slotting Approach'))))
    OR ("{v_row_id}" NOT IN ('R030','R045','R140','R150')
    AND UPPER (NVL (RWA_CALC_METHOD, 'x')) <> UPPER ('AIRB - SL Slotting Approach')))
    GROUP BY 
    CASE WHEN COREP_ISO_CODE = 'CK' AND REPORTING_REGULATOR_CODE = 'CBI' THEN 'OC' ELSE COREP_ISO_CODE END,
    RISK_TAKER_GROUP,
    RISK_TAKER_GROUP,
    YEAR_MONTH_RK,
    REPORTING_REGULATOR_CODE'''
    
    df= spark.sql(query)
    return df
  
def run_trs_corep_09_02_stg_2(v_year_month_rk,p_return_rk,v_ytd,v_row_id,v_exp_class1,v_exp_class2,v_exp_class3,v_exp_class4,v_exp_class5):
    query=f'''SELECT 
    {v_year_month_rk} YEAR_MONTH_RK,
    {p_return_rk} RETURN_RK,
    "{v_row_id}" row_id,
    cast(NULL as double) AS C010,
    cast(NULL as double) AS C030,
    cast(NULL as double) AS C040,
    cast(NULL as double) AS C050,
    cast(NULL as double) AS C055,
    SUM (IMP_WRITE_OFF_AMT) AS C060,
    cast(NULL as double) AS C070,
    cast(NULL as double) AS C080,
    cast(NULL as double) AS C090,
    cast(NULL as double) AS C100,
    cast(NULL as double) AS C105,
    cast(NULL as double) AS C110,
    cast(NULL as double) AS C120,
    cast(NULL as double) AS C125,
    cast(NULL as double) AS c130,
    REPORTING_REGULATOR_CODE AS REPORTING_REGULATOR_CODE,
    RISK_TAKER_GROUP AS RISK_TAKER_GROUP,
    NULL AS  FOR_ADJUST_FLAG,
    CASE WHEN COREP_ISO_CODE = 'CK' AND REPORTING_REGULATOR_CODE = 'CBI' THEN 'OC' ELSE COREP_ISO_CODE END AS ISO_CODE
    FROM  
    temp_table_trs_09_02
    WHERE
    YEAR_MONTH_RK IN (SELECT YEAR_MONTH_RK FROM {db}.{tbl_dim_year_month} WHERE YEAR = {v_ytd} AND YEAR_MONTH_RK < {v_year_month_rk})
    AND NVL(IMP_WRITE_OFF_AMT, 0) != 0 
    AND REG_EXPOSURE_TYPE IN(nvl("{v_exp_class1}",REG_EXPOSURE_TYPE),"{v_exp_class2}","{v_exp_class3}","{v_exp_class4}","{v_exp_class5}")
    AND (("{v_row_id}" = 'R140' AND UPPER (RWA_CALC_METHOD) = UPPER ('AIRB - PD/LGD Approach')) 
    OR ("{v_row_id}" = 'R045' AND UPPER (RWA_CALC_METHOD) = UPPER ('AIRB - SL Slotting Approach'))
    OR ("{v_row_id}" IN ('R030','R150') AND ((REG_EXPOSURE_TYPE = 'Corporates - Specialised Lending' 
    AND UPPER (RWA_CALC_METHOD) = UPPER ('AIRB - SL Slotting Approach'))
    OR (UPPER (NVL (RWA_CALC_METHOD, 'x')) <> UPPER ('AIRB - SL Slotting Approach'))))
    OR ("{v_row_id}" NOT IN ('R030','R045','R140','R150')
    AND UPPER (NVL (RWA_CALC_METHOD, 'x')) <> UPPER ('AIRB - SL Slotting Approach')))
    GROUP BY 
    CASE WHEN COREP_ISO_CODE = 'CK' AND REPORTING_REGULATOR_CODE = 'CBI' THEN 'OC' ELSE COREP_ISO_CODE END,
    RISK_TAKER_GROUP,
    YEAR_MONTH_RK,
    REPORTING_REGULATOR_CODE'''
    
    df=spark.sql(query)
    return df
  
def unpivot_trs_corep_09_02(df_first_insert_rowid10):
    
    unpivot_col="stack(15,'C010',C010,'C030',C030,'C040',C040,'C050',C050,'C055',C055,'C060',C060,'C070',C070,'C080',C080,'C090',C090,'C100',C100,'C105',C105,'C110',C110,'C120',C120,'C125',C125,'C130',C130) as (column_id,value)"
    print("end of stack")
    unpivot_df=df_first_insert_rowid10.select('year_month_rk',"return_rk","row_id","reporting_regulator_code","risk_taker_group","for_adjust_flag","iso_code",expr(unpivot_col))
    return unpivot_df
  
 def run_trs_corep_09_02_stg_3(v_year_month_rk,p_return_rk,v_row_id,v_exp_class1,v_exp_class2,v_exp_class3,v_exp_class4,v_exp_class5):
    query=f'''SELECT 
    {v_year_month_rk} YEAR_MONTH_RK,
    {p_return_rk} RETURN_RK,
    "{v_row_id}" ROW_ID,
    SUM (B2_IRB_ORIG_EXP_PRE_CON_FACTOR) AS C010,
    SUM (CASE WHEN MGS = 27 THEN B2_IRB_ORIG_EXP_PRE_CON_FACTOR END) AS C030,
    SUM (OBSRVD_NEW_DEFAULT_EXP_AMT) AS C040,
    cast(NULL as double) AS C050,
    SUM (B2_IRB_PROVISIONS_AMT) AS C055,
    SUM (IMP_WRITE_OFF_AMT) AS C060,
    SUM (OBSRVD_NEW_DEFAULT_CR_ADJ) AS c070,
    SUM (EAD_PD_NUMERATOR )/NULLIF(SUM(EAD_POST_CRM_AMT),0) as C080,
    SUM (EAD_LGD_NUMERATOR)/NULLIF(SUM(EAD_POST_CRM_AMT),0)  as C090,
    SUM (CASE WHEN MGS = 27 THEN EAD_LGD_NUMERATOR END)/NULLIF(SUM( CASE WHEN MGS = 27 THEN EAD_POST_CRM_AMT END),0) as C100,
    SUM (EAD_POST_CRM_AMT) AS C105,
    SUM (RWA_POST_CRM_PRE_SPRT_AMOUNT ) AS C110,
    SUM ( CASE WHEN MGS = 27 THEN RWA_POST_CRM_PRE_SPRT_AMOUNT END) AS C120,
    SUM (RWA_POST_CRM_AMOUNT ) AS C125,
    SUM (B2_APP_EXPECTED_LOSS_AMT) AS c130,
    REPORTING_REGULATOR_CODE AS REPORTING_REGULATOR_CODE,
    RISK_TAKER_GROUP AS RISK_TAKER_GROUP,
    NULL as  FOR_ADJUST_FLAG,
    'ALL' AS ISO_CODE
    FROM temp_table_trs_09_02
    WHERE
    YEAR_MONTH_RK = {v_year_month_rk}
    AND REG_EXPOSURE_TYPE IN (nvl("{v_exp_class1}",REG_EXPOSURE_TYPE), "{v_exp_class2}", "{v_exp_class3}", "{v_exp_class4}", "{v_exp_class5}")
    AND (("{v_row_id}" = 'R140' AND UPPER (RWA_CALC_METHOD) = UPPER ('AIRB - PD/LGD Approach')) OR ("{v_row_id}" = 'R045' AND UPPER (RWA_CALC_METHOD) = UPPER ('AIRB - SL Slotting Approach')) OR ("{v_row_id}" IN ('R030','R150') AND ((REG_EXPOSURE_TYPE = 'Corporates - Specialised Lending' AND UPPER (RWA_CALC_METHOD) = UPPER ('AIRB - SL Slotting Approach')) OR  (UPPER (NVL (RWA_CALC_METHOD, 'x')) <>UPPER ('AIRB - SL Slotting Approach')))) OR ("{v_row_id}" NOT IN ('R030','R045','R140','R150') AND UPPER (NVL (RWA_CALC_METHOD, 'x')) <> UPPER ('AIRB - SL Slotting Approach')))
    GROUP BY
    RISK_TAKER_GROUP,
    YEAR_MONTH_RK,
    REPORTING_REGULATOR_CODE'''
    
    df=spark.sql(query)
    return df
  
 # running the proc
 proc_dpm_corep_09_02()
  
 # output
#20211130
#202111
#in if condition
#in second union
#starting stack
#end of stack

spark.sql(f"select distinct c030 from corep_stg").show()

spark.sql("select cast(NULL as double) ").printSchema()



