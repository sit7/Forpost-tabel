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



