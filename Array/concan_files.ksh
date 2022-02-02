#!/bin/ksh
#######################################################################
# Name        : rtr_ven_risk_fileloader_preproc.ksh
# File type   : Korn Shell Script.
# Description : Pre Proc Procedure to merge venture risk file
# Author      : KUNAL BUDDHA
# Date        : 31-JUL-2012
#######################################################################
# Date$
# Revision$
# Last Editor$

LOGDIR=${HOME}/Workspace/logs/
LOG_FILE="${LOGDIR}`basename $0`.`date '+%Y%m%d'`.log"
#echo $0
#echo $LOG_FILE
BUSINESSDATE=${1}

echo  "Start of logs for $BUSINESSDATE" >> ${LOG_FILE}

if [ -f $FTPDIR/risk/MASTER_ven_stress.go ]
then
{

    # for handling case when file loader is re-run
    if [ -f $FTPDIR/risk/MASTER_ven_stress_${BUSINESSDATE}.dat ]
    then
    {
        cp $FTPDIR/risk/MASTER_ven_stress_${BUSINESSDATE}.dat $FTPDIR/risk/MASTER_ven_stress.dat
        echo 'Case Handled when FileLaoder was Re-Run' >> ${LOG_FILE}
       }
    else
    {
     #Unzip all compressed file
     if [ -f $FTPDIR/risk/FOR_${BUSINESSDATE}*CONCATENATED.dat.bz2 ]
     then
     {
        #echo "Unzip all files " >> ${LOG_FILE}
	for inputdatfile in `ls $FTPDIR/risk/FOR_${BUSINESSDATE}*CONCATENATED.dat.bz2`
        do
        {
         bunzip2 $inputdatfile
        }
        done
       }
       fi
     # Processing all files
     if [ -f $FTPDIR/risk/FOR_${BUSINESSDATE}*CONCATENATED.dat ]
     then
      {
      #echo "begin loop"
      for inputxmlfile in `ls $FTPDIR/risk/FOR_${BUSINESSDATE}*CONCATENATED.dat`
      do
       {
        #echo "in loop"
        cat $inputxmlfile >> $FTPDIR/risk/MASTER_ven_stress_${BUSINESSDATE}.dat
        # Copied file here and not after loop as there will be script error if we have memory full issue and concatenation didn''t work for some reason
        cp $FTPDIR/risk/MASTER_ven_stress_${BUSINESSDATE}.dat $FTPDIR/risk/MASTER_ven_stress.dat
            echo " Created MASTER_ven_stress.dat after concatenating all xmls"  >> ${LOG_FILE}
         echo $?
         if [ $? -eq 0 ]
         then
         {
           echo "Concatenated file < $inputxmlfile > for Business Date < $BUSINESSDATE > in Master file" >> ${LOG_FILE}
           echo $inputxmlfile
           mv $inputxmlfile ${inputxmlfile}.done
           gzip  ${inputxmlfile}.done  
           echo "Moved file < $inputxmlfile > to < ${inputxmlfile}.done for Business Date < $BUSINESSDATE > in Master file" >> ${LOG_FILE}

         }
         else
         #for logging error if there is memory full issue
         {
          echo "Exiting while forming master file for Business Date < $BUSINESSDATE > for file < $inputxmlfile >" >> ${LOG_FILE}
          exit -1
         }
         fi
       }
       #echo "before done"
       done
       }
              else
              {  #for handling case when there are no input files on a day
                   if [ -f $FTPDIR/risk/MASTER_ven_stress.dat ]
                   then
                   {
                    rm $FTPDIR/risk/MASTER_ven_stress.dat
                   }
                   fi
                   touch $FTPDIR/risk/MASTER_ven_stress_${BUSINESSDATE}.dat
                   cp $FTPDIR/risk/MASTER_ven_stress_${BUSINESSDATE}.dat $FTPDIR/risk/MASTER_ven_stress.dat
                   echo " No input xml files for < $BUSINESSDATE > " >> ${LOG_FILE}
              }
             fi
            }
            fi
       }
       fi
echo  "End of logs for $BUSINESSDATE " >> ${LOG_FILE}
