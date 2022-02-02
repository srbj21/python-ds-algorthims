#!/bin/ksh
# Description: Script to publish the Lising Impact file as part of RTR project
# Author:      Mohit Anand
# Date:        03/May/2010
# Version:     1
# Requirements RTR Impact Extracts are required to be published to Group Treasury 
#		through Argon EMS Main
# Change History
# Date        Who               Description
# ----------  ----------------  -----------
# 03/05/2010	Mohit Anand	Initial Version 
# -------------------------------------------------------------------------
UPLOADFILE=$1
BASE_DIR="${HOME}/extracts"
SCRIPT=$(basename ${0})
ARGON_ROOT_DIR=${SCRIPTS}/Argon
EXTRACT_DIR=${BASE_DIR}/
ARCHIVE_DIR=${BASE_DIR}/
LOG_FILE=${OST_LOGS}/${SCRIPT}.$(date '+%Y%m%d').log

#################################################################
# Generic logging                                               #
#################################################################
log_msg()
{
        TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
        print -R "[${TIMESTAMP}] ${*}" >> ${LOG_FILE}
}

echo "==============================================" >> ${LOG_FILE}

echo "Starting the Argon FTP Processing for RTR Listing File ${UPLOADFILE}" >> ${LOG_FILE}

echo "Starting the Directory Validation " >> ${LOG_FILE}

#################################################################
# Environment checks                                            #
#################################################################
if [ ! -d "${EXTRACT_DIR}" ]
then
        log_msg "Extract directory '${EXTRACT_DIR}' not found.
        exit 1
fi
if [ ! -d "${ARCHIVE_DIR}" ]
then
        log_msg "Archive directory '${ARCHIVE_DIR}' not found.
        exit 1
fi

if [ ! -d "${ARGON_ROOT_DIR}" ]
then
        log_msg "Argon client root directory '${ARGON_ROOT_DIR}' not found."
        exit 1
fi

echo "Completing the Directory Validation Sucessfully FOR RTR LISTING FILE" >> $LOG_FILE

#################################################################
# rtr_listing_file process                                      #
#################################################################

log_msg "Ready to upload  RTR LIST FILE : ${UPLOADFILE}"

#
# Upload extract file to Argon queue
#

echo " Starting the  File Validation For RTR Listing File" >> $LOG_FILE

if [ -f "${EXTRACT_DIR}/${UPLOADFILE}" ]
then
				echo "Completing the  File Validation Sucessfully For RTR Listing File" >> $LOG_FILE

				cd ${ARGON_ROOT_DIR}
				log_msg "Calling Argon FTP client in: ${ARGON_ROOT_DIR}"

				#
				# Call Argon FTP client
				#
				ArgonFTP.sh -d $EXTRACT_DIR file_xfer -v 3 $UPLOADFILE 2>&1 >> ${LOG_FILE}

				RET=$?

				if [[ ${RET} -ne 0 ]] then
								log_msg "Argon FTP client returned error code [${RET}]."
								exit ${RET}
				else
								log_msg "Argon FTP client returned success."
				fi

				#
				# Archive uploaded extract file
				#
#				log_msg "Archiving uploaded file: $UPLOADFILE"
#                mv -f ${EXTRACT_DIR}/${UPLOADFILE} ${ARCHIVE_DIR}

else
				log_msg "RTR LIST FILE '${UPLOADFILE}' not found."
				log_msg " Aborting ArgonFTp processing for RTR List FILE '${UPLOADFILE}' not found."
				exit -1
fi


echo "Completed ArgonFTP For RTR Listing File $UPLOADFILE} Successfully" >> $LOG_FILE
