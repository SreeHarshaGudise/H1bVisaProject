bag1 = load '/user/hive/warehouse/project.db/harsha1' using PigStorage() as (s_no:int,case_status:chararray,employer_name:chararray,soc_name:chararray,job_title:chararray,full_time_position:chararray,prevailing_wage:int,year:chararray,worksite:chararray,longitude:double,latitude:double);
bag2 = group bag1 by (year,job_title,full_time_position);
bag3 = foreach bag2 generate group as bagcount,COUNT(bag1) as total;
bag4 = group bag1 by (year,job_title,full_time_position);
bag5 = foreach bag4 generate group as bagsum,SUM(bag1.prevailing_wage) as wagesum;
bag6 = join bag3 by bagcount,bag5 by bagsum;
bag7 = foreach bag6 generate $0,$1,$3;
bag8 = foreach bag7 generate FLATTEN($0),(double)$2/$1;
dump bag8;


