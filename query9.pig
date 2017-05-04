bag1 = load '/user/hive/warehouse/project.db/harsha1' using PigStorage() as (s_no:int,case_status:chararray,employer_name:chararray,soc_name:chararray,job_title:chararray,full_time_position:chararray,prevailing_wage:int,year:chararray,worksite:chararray,longitude:double,latitude:double);
total = group bag1 by employer_name;
empcount = foreach total generate group,COUNT(bag1) as total;
bag2 = filter empcount by $1 > 1000;
bag2 = foreach bag2 generate FLATTEN(group) as employer_name,FLATTEN(total);
bag3 = filter bag1 by case_status == 'CERTIFIED';
bag3 = group bag3 by employer_name;
bag3 = foreach bag3 generate FLATTEN(group) as employer_name,COUNT(bag3);
bag4 = filter bag1 by case_status == 'CERTIFIED-WITHDRAWN';
bag4 = group bag4 by employer_name;
bag4 = foreach bag4 generate FLATTEN(group) as employer_name,COUNT(bag4);
joined = join bag2 by $0,bag3 by $0,bag4 by $0;
optimized = foreach joined generate $0,$1,$3,$5;
success = foreach optimized generate $0,($2+$3)*100/$1 as  successrate;
success = filter success by $1 > 80;
success = ORDER success by $1 desc;
dump success; 





















