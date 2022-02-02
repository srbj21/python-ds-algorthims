SELECT 'Dulicate values in Fire code mapping Table' as title, CNT FROM
 (
 select count(*) CNT, WALKER_BUSINESS_UNIT_ID from RS_LIQ_WBU_COUNTRY_T  f where
   target_entity_ind='GTO'GROUP BY WALKER_BUSINESS_UNIT_ID having count(*) >1
    ) ;
 
  
 SELECT 'RTR data Swing alert between listing file and OBIEE' as title, COUNT(*) FROM
 (
 select rtr_obiee_number.AMOUNT_GBP_NON_CUM - rtr_listing_file_number.AMOUNT_GBP_NON_CUM AS DIFF_SUM_AMOUNT_GBP_NON_CUM from 
 (select round(sum(AMOUNT_GBP_NON_CUM)) AMOUNT_GBP_NON_CUM from wh_liquidity_impact_f f,wh_legal_agreement_d d   
where  business_date=(SELECT business_date FROM   stg_fdm_update_control_t sfuct  WHERE  upper(table_name) = upper('WH_LIQUIDITY_IMPACT_F'))
 AND f.business_date=la_business_date
and d.la_trigger_id=f.la_trigger_id and f.notch# >0   and NOT (la_agreement_type  in  ('Account Bank', 'Collection Agent','Cash Deposit Bank') 
OR ( LA_AGREEMENT_TYPE  ='ISDA' AND  COLLATERAL_CALC_CODE  in('THRHLD-NOTRIGGER')))
--and  d.ITRF_OTRF_USER_ACCESS_ROLE like 'NRFB%'
and maturity_band_id=0  
AND SCENARIO_ID=7 
  ) rtr_obiee_number,
 (select  sum(LEDGER_BALANCE_GROSS1)*1000000 AMOUNT_GBP_NON_CUM from fdm.RTR_FILE_LISTING_EXT_T where lm_row_ref not in ('22051','22054','22053','22050','22052')
and la_product_type in ('LIQF','RCF','MOF','GTEE','LOCR','GIC','VDRV' ,'SDRV','VFN')
  ) rtr_listing_file_number
where abs(rtr_obiee_number.AMOUNT_GBP_NON_CUM - rtr_listing_file_number.AMOUNT_GBP_NON_CUM ) > 100);

SELECT 'DST data Swing alert between listing file and OBIEE' as title, COUNT(*) FROM
 (
 select dst_obiee_number.AMOUNT_GBP_NON_CUM - dst_listing_file_number.AMOUNT_GBP_NON_CUM AS DIFF_SUM_AMOUNT_GBP_NON_CUM from 
 (select round(sum(MARKET_STRESS_NON_CUMM_GBP))  AMOUNT_GBP_NON_CUM from wh_liquidity_impact_stress_f f, wh_legal_agreement_d d  where
  f.SRC_business_date=(SELECT business_date FROM   stg_fdm_update_control_t sfuct  WHERE  upper(table_name) = upper('WH_LIQUIDITY_IMPACT_F_STRES2'))  
    and d.la_business_date =f.business_date 
 -- and d.ITRF_OTRF_USER_ACCESS_ROLE like 'RFB%' 
  and f.la_trigger_id=d.la_trigger_id
  and (f.STRESS_scenario_name ='DAILY_LIQ_13032018_HYPO_UKINFL' OR f.STRESS_scenario_name ='LIQ_HIST_22_SEP_08_10D')
and min_notch_flag='Y'   
AND SCENARIO_ID=7 and maturity_band_id=0 
  ) dst_obiee_number,
 (select sum(LEDGER_BALANCE_GROSS1)*1000000 AMOUNT_GBP_NON_CUM from fdm.RTR_DST_LISTING_FILE_T where la_product_type='ILAA13'
  ) dst_listing_file_number
where abs(dst_obiee_number.AMOUNT_GBP_NON_CUM - dst_listing_file_number.AMOUNT_GBP_NON_CUM ) > 100);
