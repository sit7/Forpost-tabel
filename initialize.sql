CREATE EXTENSION mysql_fdw;

CREATE SERVER mysql_server FOREIGN DATA WRAPPER mysql_fdw OPTIONS (host '192.168.106.157', port '3306');

CREATE USER MAPPING FOR postgres SERVER mysql_server OPTIONS (username 'petrovaa', password 'h272jL37');

CREATE FOREIGN TABLE cctv_event (
	  ID int NOT NULL,
	  CameraID int,
	  CreationDate timestamp,
	  EventSubjectID int,
	  EventTypeID int,
	  Param text,
	  StreamerID int,	
	  UserIP bigint
) SERVER mysql_server OPTIONS (dbname 'cctv', table_name 'Event');

CREATE FOREIGN TABLE cctv_face_online (
  PersonID int,
  FaceDescriptorID int
 ) SERVER mysql_server OPTIONS (dbname 'cctv', table_name 'FaceOnline');

CREATE FOREIGN TABLE cctv_face_person (
  ID int,
  Name varchar(50)
 ) SERVER mysql_server OPTIONS (dbname 'cctv', table_name 'FacePerson');


insert into cctv_person (person_id, person_name,t_number)
select id, name, 0 from cctv_face_person where length(name)>5;

-- заполнение таблицы рабочего времени на основе серверного распознавания лиц
CREATE OR REPLACE FUNCTION public.cctv_upload_person_worktime(timestamp without time zone)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
BEGIN
with work_time as 
(
select cctv_faceonline.personid, cctv_event.creationdate
from cctv_faceonline
	inner join cctv_event on cctv_faceonline.FaceDescriptorID = regexp_replace(substring(cctv_event.Param from '(FaceDescriptorID\":\d*)'), '(FaceDescriptorID\":)', '')::int
where 	 DATE(CreationDate)=$1 AND EventSubjectID =521 and cameraid=9 -- событие серверной идентификации, 9 - номер нашей камеры
)
INSERT INTO public.cctv_person_worktime(person_id, work_date, begin_time, end_time, time_in_hours)
select c.person_id, $1, min(wt.creationdate) as begin_time, max(wt.creationdate) as end_time, 
 (EXTRACT(epoch from age(MAX(wt.creationdate), MIN(wt.creationdate))) / 3600)::decimal(3,1) as time_in_hours
from cctv_person c 
	left join work_time wt on c.person_id = wt.personid
group by  c.person_id, c.person_name, DATE(CreationDate);
END
$function$
;

-- заполнение таблицы рабочего времени на основе терминального распознавания лиц
CREATE OR REPLACE FUNCTION public.cctv_upload_person_worktime_terminal(timestamp without time zone)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
BEGIN
with work_time as 
(
select regexp_replace(substring(cctv_event.Param from '(ExternalPersonID\":\d*)'), '(ExternalPersonID\":)', '')::int AS external_personid, cctv_event.creationdate
from cctv_event
where 	 DATE(CreationDate)=$1 AND EventSubjectID =515 and cameraid=9
)
INSERT INTO public.cctv_person_worktime(person_id, work_date, begin_time, end_time, time_in_hours)
select c.person_id, $1, min(wt.creationdate) as begin_time, max(wt.creationdate) as end_time, 
 (EXTRACT(epoch from age(MAX(wt.creationdate), MIN(wt.creationdate))) / 3600)::decimal(3,1) as time_in_hours
from cctv_person c 
	left join work_time wt on c.external_personid = wt.external_personid
group by  c.person_id, c.person_name, DATE(CreationDate);
END
$function$
;


-- возвращает журнал рабочего времени в виде PIVOT-таблицы
CREATE OR REPLACE FUNCTION public.cctv_get_tabel(integer, integer)
 RETURNS TABLE(person_name text, person_id integer, d01 numeric, d02 numeric, d03 numeric, d04 numeric, d05 numeric, d06 numeric, d07 numeric, d08 numeric, d09 numeric, d10 numeric, d11 numeric, d12 numeric, d13 numeric, d14 numeric, d15 numeric, d16 numeric, d17 numeric, d18 numeric, d19 numeric, d20 numeric, d21 numeric, d22 numeric, d23 numeric, d24 numeric, d25 numeric, d26 numeric, d27 numeric, d28 numeric, d29 numeric, d30 numeric, d31 numeric)
 LANGUAGE sql
AS $function$

select cp.person_name, t.*
from cctv_person cp  inner join
(SELECT *
FROM crosstab(
  $$select person_id, date_part('day',work_date) as dd, time_in_hours from cctv_person_worktime where date_part('year',work_date)=$$ || $1 || $$ and date_part('month',work_date)=$$|| $2 ||$$ order by 1$$,
  $$select d from generate_series(1,31) d$$
) as (
  person_id int4, "d01" numeric, "d02" numeric, "d03" numeric, "d04" numeric, "d05" numeric, "d06" numeric, "d07" numeric, "d08" numeric, "d09" numeric, "d10" numeric,
  "d11" numeric, "d12" numeric, "d13" numeric, "d14" numeric, "d15" numeric, "d16" numeric, "d17" numeric, "d18" numeric, "d19" numeric, "d20" numeric,
  "d21" numeric, "d22" numeric, "d23" numeric, "d24" numeric, "d25" numeric, "d26" numeric, "d27" numeric, "d28" numeric, "d29" numeric, "d30" numeric, "d31" numeric
)) AS t on cp.person_id =t.person_id 
order by 1;

$function$
;



