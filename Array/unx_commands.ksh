*************************
uncompress .gz files
*************************

gzip -d filename


*************************
uncompress .bz2 files
*************************

bzip2 -d filename


********************************************************
Find and replace text within a file using sed command
********************************************************

Use Stream EDitor (sed) as follows:

sed -i 's/old-text/new-text/g' input.txt

The s is the substitute command of sed for find and replace

It tells sed to find all occurrences of ‘old-text’ and replace with ‘new-text’ in a file named input.txt

Verify that file has been updated:
more input.txt


*******************--------------------
row count 
*******************--------------------

wc -l filename

****************-------------------------------
Bad record Unix command
****************-------------------------------
awk -F'~' '{print  $1}' MASTER_ven_dst_stress_20210222.dat | awk '{ if ( length > 50 ) print NR, length, $1}' 
awk -F'~' '{print  $1}' MASTER_ven_dst_stress_20180226.dat | awk '{ if ( length > 100 ) print NR, length, $0}' 


cat MASTER_ven_dst_stress.dat | awk '{print length($0), $0}' | sort -g | tail -5


awk 'length>100000' MASTER_ven_dst_stress_20210222.dat

**********************************************
remove specific record from file
**********************************************
sed '/NPV~-3053.670~~L210219A47F9D~EUR/d' MASTER_ven_dst_stress_20210222.dat > output_file


**********************************
to check running process in unix
**********************************
ps -aux

***************************
to kill all session in Unix
****************************

pkill -u username

killall -u username
