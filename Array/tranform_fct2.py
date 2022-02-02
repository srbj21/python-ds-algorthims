from pyspark.sql import SparkSession
from datetime import datetime
db='rtd_mart'
tbl_crncy_cnversion_fctr='currency_conversion_factor'
tbl_etl_param='etl_parameter'
tbl_dim_year_month='dim_year_month'
tbl_dim_rwa_engine='dim_rwa_engine_generic_params'
tbl_dim_asset_class='dim_asset_class'
tbl_fct_corep_crd4='fct_corep_crd4'
tbl_dim_counterparty='dim_counterparty'

def get_currency(currency_code,currency_date):
    query='''select fx_rate from {0}.{1} where currency_code='{2}' and '{3}' >= valid_from_date and 
    '{3}' < valid_to_date'''.format(db,tbl_crncy_cnversion_fctr,currency_code,currency_date)
    fx_rate=spark.sql(query).collect()[0][0]
    
    if fx_rate is None:
        return 0
    else:
        return fx_rate
    

#out=get_currency('USD','2021-10-12')
#print(out)
#print(type(out))

def get_core_reporting_date():
    rep_date_param='REPORTING_DATE'
    query =f'''select to_date(from_unixtime(unix_timestamp(parameter_value,"dd-MMM-yyyy"))) from 
    {db}.{tbl_etl_param} where parameter_name="{rep_date_param}" '''
    rep_date=spark.sql(query).collect()[0][0]
    print(type(rep_date))
    return rep_date


#out=get_core_reporting_date()
#print(out)

def get_year_month_rk(p_reporting_date):
    query=f'''select year_month_rk from {db}.{tbl_dim_year_month} where year= year("{p_reporting_date}") and 
    month=month("{p_reporting_date}")'''
    year_month_rk = spark.sql(query).collect()[0][0]
    return year_month_rk

#out=get_year_month_rk('2021-10-12')
#print(type(out))
#print(out)

def get_icb_effective_date():
    icb_date_param='ICB_EFFECTIVE_DATE'
    query = f'''select date_format(to_date(from_unixtime(unix_timestamp(parameter_value,"dd-MMM-yyyy"))),"YYYYMM") from 
    {db}.{tbl_etl_param} where parameter_name="{icb_date_param}" '''
    icb_effective_date=spark.sql(query).collect()[0][0]
    return int(icb_effective_date)

#out=get_icb_effective_date()
#print(type(out))
#print(out)

def get_date_from_etl_parameter(parameter_name):
    query=f'''select date_format(to_date(from_unixtime(unix_timestamp(parameter_value,"dd-MMM-yyyy"))),"YYYYMM") FROM 
    {db}.{tbl_etl_param} WHERE parameter_name = "{parameter_name}" '''
    etl_param_date = spark.sql(query).collect()[0][0]
    return int(etl_param_date)

#v_core_ig_eff_year_month_rk = get_date_from_etl_parameter('CORE_TO_CORE_IG_EXP_EFF_DATE')
#print(type(v_core_ig_eff_year_month_rk))
#print(v_core_ig_eff_year_month_rk)

def get_discount_factor_from_dregp(param_code):
    query=f'''select param_value from {db}.{tbl_dim_rwa_engine} where param_code = "{param_code}" '''
    param_value=spark.sql(query).collect()[0][0]
    return int(param_value)
  
def get_prev_qtr_date(rep_date):
    query =f'''select date_format(qtr_end_dt,"YYYYMM") from (select distinct quarter_end_date as qtr_end_dt from 
    {db}.{tbl_dim_year_month} where month_end_date= last_day(add_months("{rep_date}",-3)))a'''
    qtr_end_dt=spark.sql(query).collect()[0][0]
    return qtr_end_dt

#out=get_prev_qtr_date('2021-12-23')
#print(out)
#print(type(out))

def get_prev_month_end_date(rep_date):
    query=f'''select date_format(mon_end_dt,"YYYYMM") from (select distinct month_end_date as mon_end_dt from 
    {db}.{tbl_dim_year_month} where month_end_date= last_day(add_months("{rep_date}",-1)))a'''
    mon_end_dt=spark.sql(query).collect()[0][0]
    return mon_end_dt

#out=get_prev_month_end_date('2021-12-23')
#print(out)
#print(type(str))

def get_asset_class_rk(rep_date):
    try:
        query=f''' select asset_class_rk from {db}.{tbl_dim_asset_class} where type ="B2_STD" and valid_from_date <="{rep_date}" 
        and valid_to_date > "{rep_date}" and basel_exposure_type ="Other items" '''
        asset_class=spark.sql(query).collect()[0][0]
    except Exception as e:
        return -1
    
    else:
        return asset_class

#out=get_asset_class_rk('2021-12-20')
#print(out)
#print(type(out))


def func_get_prev_qtr(v_prev_qtr_year_month_rk,p_data_set):
    print(v_prev_qtr_year_month_rk)
    print(p_data_set)
    query=f'''SELECT
    DISTINCT
    REPORTING_REGULATOR_CODE,
    CIS_CODE,
    RWA_APPROACH_CODE,
    CASE WHEN F.RWA_APPROACH_CODE = 'STD' AND DAC_PREV.BASEL_EXPOSURE_TYPE ='Exposures in default' THEN 'Y' ELSE 'N' END STD_PRE_DFLT,
    CASE WHEN F.RWA_APPROACH_CODE = 'AIRB' AND F.B2_APP_ADJUSTED_PD_RATIO = 1 THEN 'Y' ELSE 'N' END AIRB_PRE_DFLT
    FROM {db}.{tbl_fct_corep_crd4} f
    JOIN {db}.{tbl_dim_counterparty} DC_PREV ON (F.COUNTERPARTY_RK=DC_PREV.COUNTERPARTY_RK AND F.COUNTERPARTY_RK <> -1)
    JOIN {db}.{tbl_dim_asset_class} DAC_PREV ON (F.b2_std_asset_class_rk = DAC_PREV.asset_class_rk)
    WHERE f.YEAR_MONTH_RK = {v_prev_qtr_year_month_rk} AND f.FLOW_TYPE = 'EXPOSURE' AND f.ADJUST_FLAG = "{p_data_set}"
    AND ((F.RWA_APPROACH_CODE = 'STD' AND DAC_PREV.BASEL_EXPOSURE_TYPE <> 'Exposures in default') OR (F.RWA_APPROACH_CODE = 'AIRB'
    AND NVL (F.B2_APP_ADJUSTED_PD_RATIO, 0.2501) <> 1))
    AND DC_PREV.CIS_CODE IS NOT NULL'''
    
    df=spark.sql(query)
    return df


#df=func_get_prev_qtr(202109,'A')
#df.show()

def run_transform_fct_corep_crd4(reporting_date_override=None, p_data_set='A'):
    if reporting_date_override is None:
        rdate=get_core_reporting_date()
    else:
        rdate_temp=datetime.strptime(reporting_date_override,'%Y-%m-%d')
        rdate=rdate_temp.date()
    #ryear_month_rk = get_year_month_rk(rdate)
    #v_exch_rate_eur = get_currency('EUR',reporting_date_override)
    #v_exch_rate_usd = get_currency('USD',reporting_date_override)
    #v_icb_eff_from_year_month_rk = get_icb_effective_date()
    #v_core_ig_eff_year_month_rk = get_date_from_etl_parameter('CORE_TO_CORE_IG_EXP_EFF_DATE')
    #v_infra_discount_factor= get_discount_factor_from_dregp('INFRADISCOUNTFACTOR')
    #
    v_prev_qtr_year_month_rk=get_prev_qtr_date(rdate)
    print(f"prev quarter value is {v_prev_qtr_year_month_rk}")
    print(f"p_data_Set value is {p_data_set}")
    #v_prev_mnth_year_month_rk=get_prev_month_end_date(rdate)
    #v_oth_itm_asset_class_rk = get_asset_class_rk(rdate)
    df=func_get_prev_qtr(v_prev_qtr_year_month_rk,p_data_set)
    print(type(df))
    df.show()
    
    

run_transform_fct_corep_crd4('2021-12-23')

#rdate value is 2021-12-23
#prev quarter value is 202109
#p_data_Set value is A
#202109
#A
#<class 'pyspark.sql.dataframe.DataFrame'>
