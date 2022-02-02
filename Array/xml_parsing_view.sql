CREATE OR REPLACE FORCE VIEW RTR_CURRENTSTATEMSG_TRIGGER_V
(
   LEGAL_AGREEMENT_REF,
   EVENTDATETIME,
   CHANGE_TYPE,
   RATINGAGENCY,
   RATINGTYPE,
   RATINGVALUE,
   GRACEPERIODDAYS
)
AS
   SELECT legal_agreement_ref,
          eventDateTime,
          Change_Type,
          ratingAgency,
          ratingType,
          ratingValue,
          graceperiodDays
     FROM (  SELECT legal_agreement_ref,
                    TO_DATE (eventDateTime, 'yyyy-mm-dd hh24:mi:ss ')
                       eventDateTime,
                    Change_Type,
                    ratingAgency,
                    ratingType,
                    ratingValue,
                    partyid,
                    FLOOR (MIN (MIN (graceperiodDays))
                              OVER (PARTITION BY legal_agreement_ref,
                                                 eventDateTime,
                                                 Change_Type,
                                                 ratingAgency,
                                                 ratingType,
                                                 ratingValue))
                       graceperiodDays
               FROM (SELECT    SUBSTR (legal_ref,
                                       1,
                                       INSTR (legal_ref, '/') - 1)
                            || SUBSTR (legal_ref, INSTR (legal_ref, '/') + 1)
                               legal_agreement_ref,
                               SUBSTR (eventDateTime, 1, 10)
                            || ' '
                            || SUBSTR (eventDateTime, 12, 8)
                               eventDateTime,
                            'CURRENTSTATE' Change_Type,
                            ratingAgency,
                            ratingType,
                            ratingValue,
                            partyid,
                            CASE
                               WHEN legalAgreementperiod = 'Calendar Days'
                               THEN
                                  TO_NUMBER (periodMultiplier)
                               WHEN legalAgreementperiod = 'Business Days'
                               THEN
                                  TO_NUMBER (periodMultiplier) * (7 / 5)
                               WHEN legalAgreementperiod =
                                       'Local Business Days'
                               THEN
                                  TO_NUMBER (periodMultiplier) * (7 / 5)
                               WHEN legalAgreementperiod = 'Weeks'
                               THEN
                                  TO_NUMBER (periodMultiplier) * 7
                               WHEN legalAgreementperiod = 'Months'
                               THEN
                                  TO_NUMBER (periodMultiplier) * 30
                               ELSE
                                  NULL
                            END
                               graceperiodDays
                       FROM (SELECT EXTRACTVALUE (
                                       VALUE (la),
                                       '*/legalAgreementHeader/legalAgreementIdentifier/legalAgreementId')
                                       LEGAL_REF,
                                    EXTRACTVALUE (
                                       VALUE (la),
                                       '*/messageHeader/eventDateTime')
                                       eventDateTime,
                                    UPPER (
                                       EXTRACTVALUE (
                                          VALUE (ae),
                                          '*/triggerConditions/ratingAgencyTriggerEvent/ratingAgencyTrigger/ratingAgency'))
                                       ratingAgency,
                                    EXTRACTVALUE (VALUE (op),
                                                  '*/legalAgreementPeriod')
                                       legalAgreementPeriod,
                                    EXTRACTVALUE (VALUE (op),
                                                  '*/periodMultiplier')
                                       periodMultiplier,
                                    EXTRACTVALUE (VALUE (rtg), '*/ratingType')
                                       ratingType,
                                    EXTRACTVALUE (VALUE (rtg), '*/ratingValue')
                                       ratingValue,
                                    REPLACE (
                                       UPPER (
                                          EXTRACTVALUE (VALUE (aes),
                                                        '*/applicableParty')),
                                       'PARTYID-',
                                       '')
                                       partyid
                               FROM rtr_sds_msgs_staging,
                                    TABLE (
                                       XMLSEQUENCE (
                                          EXTRACT (rtr_sds_msgs_staging.emm,
                                                   '/emml'))) la,
                                    TABLE (
                                       XMLSEQUENCE (
                                          EXTRACT (
                                             VALUE (la),
                                             '*/currentStateLegalAgreement/applicableEvents'))) aes,
                                    TABLE (
                                       XMLSEQUENCE (
                                          EXTRACT (VALUE (aes),
                                                   '*/applicableEvent'))) ae,
                                    TABLE (
                                       XMLSEQUENCE (
                                          EXTRACT (
                                             VALUE (ae),
                                             '*/triggerConditions/ratingAgencyTriggerEvent/ratingAgencyTrigger/ratingBreachThreshold'))) rtg,
                                    TABLE (
                                       XMLSEQUENCE (
                                          EXTRACT (
                                             VALUE (ae),
                                             '*/remedies/optionParty/remedyPeriod'))) op
                              WHERE     (LOWER (
                                            EXTRACTVALUE (
                                               rtr_sds_msgs_staging.emm,
                                               '/emml/legalAgreementHeader/legalAgreementStateIdentifier[legalAgreementStateIdClassificationScheme="Lifecycle State"]/legalAgreementStateId')) IN ('executed',
                                                                                                                                                                                                   'executedrestricted',
                                                                                                                                                                                                   'executeddealspecific'))
                                    AND EXTRACTVALUE (VALUE (ae),
                                                      '*/triggerName') =
                                           'First Trigger') INNER_QUERY
                      WHERE ratingAgency IN ('SP', 'MOODYS', 'FITCH')
                     UNION
                     SELECT    SUBSTR (legal_ref,
                                       1,
                                       INSTR (legal_ref, '/') - 1)
                            || SUBSTR (legal_ref, INSTR (legal_ref, '/') + 1)
                               legal_agreement_ref,
                               SUBSTR (eventDateTime, 1, 10)
                            || ' '
                            || SUBSTR (eventDateTime, 12, 8)
                               eventDateTime,
                            'CURRENTSTATE' Change_Type,
                            ratingAgency,
                            ratingType,
                            ratingValue,
                            partyid,
                            NULL graceperioddays
                       FROM (SELECT EXTRACTVALUE (
                                       VALUE (la),
                                       '*/legalAgreementHeader/legalAgreementIdentifier/legalAgreementId')
                                       LEGAL_REF,
                                    EXTRACTVALUE (
                                       VALUE (la),
                                       '*/messageHeader/eventDateTime')
                                       eventDateTime,
                                    UPPER (
                                       EXTRACTVALUE (
                                          VALUE (ae),
                                          '*/triggerConditions/ratingAgencyTriggerEvent/ratingAgencyTrigger/ratingAgency'))
                                       ratingAgency,
                                    EXTRACTVALUE (VALUE (rtg), '*/ratingType')
                                       ratingType,
                                    EXTRACTVALUE (VALUE (rtg), '*/ratingValue')
                                       ratingValue,
                                    REPLACE (
                                       UPPER (
                                          EXTRACTVALUE (VALUE (aes),
                                                        '*/applicableParty')),
                                       'PARTYID-',
                                       '')
                                       partyid
                               FROM rtr_sds_msgs_staging,
                                    TABLE (
                                       XMLSEQUENCE (
                                          EXTRACT (rtr_sds_msgs_staging.emm,
                                                   '/emml'))) la,
                                    TABLE (
                                       XMLSEQUENCE (
                                          EXTRACT (
                                             VALUE (la),
                                             '*/currentStateLegalAgreement/applicableEvents'))) aes,
                                    TABLE (
                                       XMLSEQUENCE (
                                          EXTRACT (VALUE (aes),
                                                   '*/applicableEvent'))) ae,
                                    TABLE (
                                       XMLSEQUENCE (
                                          EXTRACT (
                                             VALUE (ae),
                                             '*/triggerConditions/ratingAgencyTriggerEvent/ratingAgencyTrigger/ratingBreachThreshold'))) rtg
                              WHERE     (LOWER (
                                            EXTRACTVALUE (
                                               rtr_sds_msgs_staging.emm,
                                               '/emml/legalAgreementHeader/legalAgreementStateIdentifier[legalAgreementStateIdClassificationScheme="Lifecycle State"]/legalAgreementStateId')) IN ('executed',
                                                                                                                                                                                                   'executedrestricted',
                                                                                                                                                                                                   'executeddealspecific'))
                                    AND EXTRACTVALUE (VALUE (ae),
                                                      '*/triggerName') =
                                           'First Trigger') INNER_QUERY
                      WHERE ratingAgency IN ('SP', 'MOODYS', 'FITCH'))
           GROUP BY legal_agreement_ref,
                    eventDateTime,
                    Change_Type,
                    ratingAgency,
                    ratingType,
                    ratingValue,
                    partyid)
    WHERE partyid IN (SELECT legal_entity_code FROM t_rtr_legal_entity);
    
    
