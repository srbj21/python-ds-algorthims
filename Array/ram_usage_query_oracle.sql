select * from v$process where serial#=14723;

select
 SID,
   operation_type                OPERATION,
   trunc(WORK_AREA_SIZE/1024)    WSIZE, 
   trunc(EXPECTED_SIZE/1024)     ESIZE,
   trunc(ACTUAL_MEM_USED/1024)   MEM, 
   trunc(MAX_MEM_USED/1024/1024/1024)      "MAX MEM", 
  -- sum(trunc(MAX_MEM_USED/1024/1024/1024) ),
   number_passes                 PASS
from
   v$sql_workarea_active where sid=123
order by
   1,2;

select
to_number(decode(SID, 5825, NULL, SID)) SID,
   operation_type                OPERATION,
   trunc(WORK_AREA_SIZE/1024)    WSIZE, 
   trunc(EXPECTED_SIZE/1024)     ESIZE,
   trunc(ACTUAL_MEM_USED/1024)   MEM, 
   trunc(MAX_MEM_USED/1024)      "MAX MEM", 
   number_passes                 PASS
from
   v$sql_workarea_active
order by
   1,2;
   
   
   select
   e.sid,
   e.username,
   e.status,
   a.uga_memory,
   b.pga_memory
from
 (select y.SID, 
  TO_CHAR(ROUND(y.value/1024),99999999) || ' KB' UGA_MEMORY 
  from 
     v$sesstat y, 
     v$statname z 
   where 
     y.STATISTIC# = z.STATISTIC# 
   and 
     NAME = 'session uga memory') a,
 (select 
     y.SID, 
     TO_CHAR(ROUND(y.value/1024),99999999) || ' KB' PGA_MEMORY   
  from 
     v$sesstat y, v$statname z 
  where 
     y.STATISTIC# = z.STATISTIC#   
  and 
     NAME = 'session pga memory') b,
v$session e
where
     e.sid=a.sid 
and 
   e.sid=b.sid 
order by
   e.status,
   a.uga_memory desc;
   
   
   
   
   
