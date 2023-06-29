/*  Прототип хранилища данных. 
 *  Технический стек: SQL/ PostgreSQL / Python / Airflow.*/

create schema oltp_src_system; /* таблицы данных, функции для заполнения, триггер ; */
create schema oltp_cdc_src_system; /* здесь храниться лог изменений CDC –  строится на триггерах таблицы источника «src_system», 
									каждая строка таблица обогащена полями operation_type и updated_dttm (время обновления);*/
create schema dwh_stage; /*первый слой прототипа хранилища данных. Получает данные из источника схемы «oltp_cdc_src_system». */
create schema dwh_ods; /* детальный слой, где мы храним все версии всех строк таблиц, дополнительные технические поля valid_from_dttm, 
						valid_to_dttm, deleted_flg, deleted_dttm; История хранится непрерывно. Здесь же храниться аналитический справочн*/
create schema report; /* слой для материализации отчета (витрины данных) для последующего использования - функции для расчетов таблиц для отчетов.*/
create extension pgcrypto;

DROP TABLE oltp_src_system.card_application_data; /*источник*/
DROP TABLE oltp_cdc_src_system.cdc_card_application_data_changes; /* лог изменений */
DROP TABLE dwh_stage.card_application_data_dwh_src; /*stage хранилища */
DROP TABLE dwh_ods.card_application_data_hist; /* детальный слой - приемник*/



/* OLTP_SRC_SYSTEM Источник*/

-- DROP TABLE oltp_src_system.card_application_data;
/* Генерируемый источник */

CREATE TABLE oltp_src_system.card_application_data (
	id int4 not NULL,
	id_client int8 not NULL,
	status_nm text NULL,
	create_dttm timestamptz NULL,
	update_dttm timestamptz NULL
);

/* OLTP_CDC_SRC_SYSTEM  Захват изменений*/

-- DROP TABLE  oltp_cdc_src_system.cdc_card_application_data_changes;
   /* Таблица захвата изменений */

CREATE TABLE oltp_cdc_src_system.cdc_card_application_data_changes ( 
	id int4 not NULL,
	id_client int8 NULL,
	status_nm text NULL,
	create_dttm timestamptz NULL,
	update_dttm timestamptz NULL,
	operation_type bpchar(1)  NULL,
	updated_dttm timestamptz  null /* время обновления*/
);

/*dwh_stage - таблица хранения истории изменений*/
-- DROP TABLE dwh_stage.card_application_data_dwh_src;

--create extension IF NOT exists pgcrypto;

CREATE TABLE dwh_stage.card_application_data_dwh_src(
	id int4 not NULL,
	id_client int8 NULL,
	status_nm text NULL,Frnefkmyst 
	create_dttm timestamptz NULL,
	operation_type bpchar(1) NULL,
	updated_dttm timestamptz  null, /* время обновления из cdc*/
	hash bytea NULL GENERATED ALWAYS AS 
	(digest((((( COALESCE(id::text, '#$%^&'::text) || 
	COALESCE(id_client::text, '#$%^&'::text)) ||
	COALESCE(status_nm, '#$%^&'::text)) ||
	date_part('epoch'::text, COALESCE(timezone('UTC'::text, create_dttm), '1990-01-01 00:00:00'::timestamp without time zone))::text) ||
	date_part('epoch'::text, COALESCE(timezone('UTC'::text, updated_dttm), '1990-01-01 00:00:00'::timestamp without time zone))::text)	
	, 'sha256'::text)) stored	
);

/* dwh_ods*/
-- DROP TABLE dwh_ods.card_application_data_hist;

CREATE TABLE dwh_ods.card_application_data_hist ( 
	id int4 not NULL,
	id_client int8 NULL,
	status_nm text NULL,
	create_dttm timestamptz NULL,
	operation_type bpchar(1)  NULL,
	updated_dttm timestamptz  null, /* время обновления в cdc*/
	hash bytea NULL,	
	valid_from_dttm timestamptz NULL,
    valid_to_dttm timestamptz NULL, 
    deleted_flg bpchar(1) null,
    deleted_dttm timestamptz NULL
);


/*Календарь */

CREATE TABLE dwh_ods.Dim_Date 
		(date_key INT PRIMARY KEY,   
		date_actual DATE NOT NULL,   
		year INT NOT NULL,   
		quarter INT NOT NULL,   
		month INT NOT NULL,   
		week INT NOT NULL,   
		day_of_month INT NOT NULL,
		day_of_week INT NOT NULL,   
		is_weekday BOOLEAN NOT NULL,   
		is_holiday BOOLEAN NOT NULL,   
		fiscal_year INT NOT NULL
);

/* ТАБЛИЦЫ - Витрины*/

/*Создание таблиц-витрин*/

--drop table report.tasks_start_in_state_count
CREATE TABLE report.tasks_start_in_state_count /* Количенстов задач получивших статус в данный день*/
		(date_day date,   
		status_nm text,   
		tasks_count int 
);

--drop table report.tasks_in_state_count
CREATE TABLE report.tasks_in_state_count /* Количенстов задач имеющих статус в данный день*/
		(date_key int,   
		date_actual date,   
		status_nm text,   
		tasks_count int 
);

--drop table report.deleted_tasks_count
CREATE TABLE report.deleted_tasks_count /* Количество задач, которые удалили в данном статусе в данный день*/
		(date_key int,   
		date_actual date,   
		status_nm text,   
		tasks_count int 
);

--drop TABLE report.deleted_tasks_in_state
CREATE TABLE report.deleted_tasks_in_state /* id задач, которые удалили в данном статусе в данный день*/
		(date_key int,   
		date_actual date,
		task_id int,    
		status_nm text 		
);


/*------------------------------------------------------------------------------------------------------------- */

/* OLTP_SRC_SYSTEM - ФУНКЦИИ ГЕНЕРАЦИИ ДАННЫХ в Источник - вставка, удаление, обновление*/

CREATE OR REPLACE FUNCTION oltp_src_system.create_tasks()
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_rc int;
    BEGIN
insert into oltp_src_system.card_application_data (id, id_client,status_nm,create_dttm, update_dttm)
select id,
	id_client,
  'created' status_nm ,
  now() create_dttm ,
  now() update_dttm
  from (select n id, 
			  (floor(random()*(10000)+1))::int id_client /*генерируем id номер клиента*/
                from generate_series((select coalesce((select max(id) /*генерируем  id  для задачи как имеющееся последнее значение+1*/
                                  		from oltp_src_system.card_application_data) + 1, 1)
                        )
                     , (select coalesce((select max(id) 
                                         from oltp_src_system.card_application_data)  + floor(random()*5+1)::int, 1)
                     	) 
                    ) n) nn;

    get diagnostics v_rc = row_count;           
    raise notice '% rows inserted into card_application_data',v_rc;
    return true;

end $function$
;

CREATE OR REPLACE FUNCTION oltp_src_system.delete_existed_task()
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_rc int;
BEGIN                   
delete from oltp_src_system.card_application_data
where id in (
			select id
               from (select id 
                          , round(random()*10) rnd /*формируем метку на удаление*/
                       from oltp_src_system.card_application_data
                    ) rnd_tbl
              where (rnd - floor(rnd/10)) = 1 /* and status_nm = 'closed'  */ /*удалению подлежат по хорошему только закрытые задачи, но для удобства гненерации убрала  */
            );
get diagnostics v_rc = row_count;           
raise notice '% rows deleted into card_application_data',v_rc;
return true;
end $function$
;

CREATE OR REPLACE FUNCTION oltp_src_system.update_existed_task() 
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$  
declare  
	v_rc int;  
	status_var int = floor(random()*(15) + 1)	 /* выбор блока для отработки, округляем до меньшего целого */;
begin
	--status_var := floor(random()*(15) + 1)	; /* выбор блока для отработки, округляем до меньшего целого */;
	if status_var in (1, 8, 15) /* созданная заявка сначала должна стать 'на рассмотрении' / 'на паузе' */
	then
		update oltp_src_system.card_application_data 
		set status_nm = case floor(random() * (2) + 1)
					when 1 then 'considered' /* рассматривается*/
					when 2 then 'paused' /* на паузе - например, нужны уточнения*/	
					else null
					end
		, update_dttm = now() /* обновляем дату на сейчас */
		where id in (select id
					from (select id, round(random()*10) rnd /*выбираем Id заявки и получаем рандомное чило до 10 , чтобы потом вставить в отбор строки (сформировать метку) */
					       from oltp_src_system.card_application_data 
					       where status_nm = 'created' ) rnd_tbl 
					            where (rnd - floor(rnd/10)) = 1); /* отбор строки для внесения изменений */	 		           
	elsif status_var in (2) /* заявка на рассмотрении может стать на паузе - например, нужны уточнения*/
		then
		update oltp_src_system.card_application_data
		set status_nm = 'paused' 
		, update_dttm = now() /* обновляем дату на сейчас */
		where id in (select id
					from (select id, round(random()*10) rnd 
					       from oltp_src_system.card_application_data 
					       where status_nm = 'considered' ) rnd_tbl 
					            where (rnd - floor(rnd/10)) = 1); 	
	elsif status_var in (3, 13) /*заявка 'на паузе' перед переходом в другое состояние должна снова быть 'на рассмотрении'*/
	then
		update oltp_src_system.card_application_data
		set status_nm = 'considered' 
		, update_dttm = now() 
		where id in (select id
					from (select id, round(random()*10) rnd 
					       from oltp_src_system.card_application_data 
					       where status_nm = 'paused')  rnd_tbl 
					            where (rnd - floor(rnd/10)) = 1); 	 					            
	elsif status_var in (6, 10, 12) /* рассматриваемые заявки могут быть либо 'отклонены' либо 'подтверждены', немного повысила вероятность попадания в этот блок кода */
		then 
			update oltp_src_system.card_application_data
			set status_nm = case floor(random() * (2) + 1) 
					when 1 then 'rejected'/* отклонено*/
					when 2 then 'approved'/* подтверждено */
					else null
					end
				, update_dttm = now() /* обновляем дату на сейчас */
				where id in (select id
					from (select id, round(random()*10) rnd 
					       from oltp_src_system.card_application_data 
					       where status_nm = 'considered' ) rnd_tbl 
					            where (rnd - floor(rnd/10)) = 1); 		            
	elsif status_var = 5 /* созданная, но еще не обработанная заявка может быть 'отменена'*/
		then 
			update oltp_src_system.card_application_data
			set status_nm = 'canceled'
				, update_dttm = now() 
				where id in (select id
					from (select id, round(random()*10) rnd 
					       from oltp_src_system.card_application_data 
					       where status_nm  = 'created') rnd_tbl 
					            where (rnd- floor(rnd/10)) = 1); 
					            
	elseif status_var in (4, 9, 14) /* заявки со статусом 'подтверждено' должна обработаться */
		then
		 	update oltp_src_system.card_application_data
			set status_nm =  'terminated'/* обработано */
				, update_dttm = now() 
				where id in  (select id
					from (select id, round(random()*10) rnd 
					       from oltp_src_system.card_application_data 
					       where status_nm  = 'approved') rnd_tbl 
					            where (rnd- floor(rnd/10)) = 1);					            
	elseif status_var in (7, 11, 16) /* заявки со статусом 'обработано', 'отказано' или 'отменено' должны стать 'закрыты' */
		then
		 	update oltp_src_system.card_application_data
			set status_nm =  'closed'/* отменено */
				, update_dttm = now() /* обновляем дату на сейчас */
				where id in (select id from oltp_src_system.card_application_data 
					       where status_nm in ('rejected', 'terminated', 'canceled') );
	end if ;         			           				            
	
	get diagnostics v_rc = row_count;
	raise notice '% rows updated into card_application_data',v_rc;
	return true;
end $function$
;

CREATE OR REPLACE FUNCTION oltp_src_system.load_to_cdc_from_card_application_data_changes() /*лог изменений в источнике */
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
    begin
        if (tg_op = 'DELETE') then
            insert into oltp_cdc_src_system.cdc_card_application_data_changes select old.*, 'D', now();
            return old;
        elsif (tg_op = 'UPDATE') then         
          insert into oltp_cdc_src_system.cdc_card_application_data_changes select new.*, 'U', now();
          return new;
        elsif (tg_op = 'INSERT') then
            insert into oltp_cdc_src_system.cdc_card_application_data_changes select new.*, 'I', now();
            return new;
        end if;
        return null;
    end;
$function$
;

/* Триггер на изменения в Истонике */

CREATE TRIGGER cdc_card_application_data
AFTER INSERT OR UPDATE OR DELETE ON oltp_src_system.card_application_data
    FOR EACH ROW EXECUTE PROCEDURE oltp_src_system.load_to_cdc_from_card_application_data_changes();

 
/* переносим лог cdc в хранилище */ 
   
CREATE OR REPLACE FUNCTION dwh_stage.load_from_cdc_src_system_card_application_data_to_stage()
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
    begin  
	    truncate table dwh_stage.card_application_data_dwh_src;
        insert into dwh_stage.card_application_data_dwh_src(
        		id,
				id_client,
				status_nm,
				create_dttm,				
				operation_type,
				updated_dttm)
        select 	id,
				id_client,
				status_nm,
				create_dttm,				
				operation_type,
				updated_dttm 
                from oltp_cdc_src_system.cdc_card_application_data_changes;                
        return true;
    end
$function$
;

/*--------------------------------------------------------------*/

/* dwh_ods*/


CREATE OR REPLACE FUNCTION dwh_ods.load_card_application_data_hist() /* загрузка истории изменений с корректировкой времени актуальности*/
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
    declare
      --  v_load_dttm timestamp = now();
    begin  
	    update dwh_ods.card_application_data_hist dst
           set valid_to_dttm = now() - interval '1 second'
         where exists (select null
                         from dwh_stage.card_application_data_dwh_src src
                        where src.operation_type in ('U', 'D') AND 
                        dst.id = src.id );        
	    insert into dwh_ods.card_application_data_hist(
            	id , 	
            	id_client,	
            	status_nm,	
            	create_dttm,	
            	operation_type,
				updated_dttm, /* время обновления из cdc*/	
				hash,		
			    valid_from_dttm,    
				valid_to_dttm,     
				deleted_flg, deleted_dttm)
		select *,	
				updated_dttm valid_from_dttm,    
				TO_TIMESTAMP('2999/12/31 23:59:59', 'YYYY/MM/DD HH24:MI:SS') valid_to_dttm, 
				case operation_type
  					 when 'D' then 'Y'
   					 else 'N'
    			 end deleted_flg,
				case operation_type
    				 when 'D' then updated_dttm
 					 else null
  				end deleted_dttm
		from dwh_stage.card_application_data_dwh_src src 
				where src.hash not in (select hash from dwh_ods.card_application_data_hist t1);  	/*условие, чтобы не записывать то, что уже есть в хранилище */		                    
        return true;
    end
$function$
;

/*Создание календаря */


CREATE OR REPLACE FUNCTION dwh_ods.create_Dim_Date() /* загрузка истории изменений с корректировкой времени актуальности*/
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
    declare
        v_load_dttm timestamp = now();
    begin  
	    
		INSERT INTO dwh_ods.Dim_Date 
			(date_key,   
			date_actual ,   
			year,   
			quarter,   
			month,   
			week,   
			day_of_month ,
			day_of_week,   
			is_weekday,   
			is_holiday,   
			fiscal_year )
		VALUES 
		/* Заполнение календаря - с учетом праздничных майских дней.*/
			(20230501, '2023/05/01 23:59:59', 2023, 2, 5, 18, 01, 2, true, true, 2023),
			(20230502, '2023/05/02 23:59:59', 2023, 2, 5, 18, 02, 3, true, false, 2023),
			(20230503, '2023/05/03 23:59:59', 2023, 2, 5, 18, 03, 4, true, false, 2023),
			(20230504, '2023/05/04 23:59:59', 2023, 2, 5, 18, 04, 5, true, false, 2023),
			(20230505, '2023/05/05 23:59:59', 2023, 2, 5, 18, 05, 6, true, false, 2023),
			(20230506, '2023/05/06 23:59:59', 2023, 2, 5, 18, 06, 7, false, true, 2023),
			(20230507, '2023/05/07 23:59:59', 2023, 2, 5, 19, 07, 1, false, true, 2023),
			(20230508, '2023/05/08 23:59:59', 2023, 2, 5, 19, 08, 2, true, true, 2023),
			(20230509, '2023/05/09 23:59:59', 2023, 2, 5, 19, 09, 3, true, true, 2023),
			(20230510, '2023/05/10 23:59:59', 2023, 2, 5, 19, 10, 4, true, false, 2023),
			(20230511, '2023/05/11 23:59:59', 2023, 2, 5, 19, 11, 5, true, false, 2023),
			(20230512, '2023/05/12 23:59:59', 2023, 2, 5, 19, 12, 6, true, false, 2023),
			(20230513, '2023/05/13 23:59:59', 2023, 2, 5, 19, 13, 7, false, true, 2023),
			(20230514, '2023/05/14 23:59:59', 2023, 2, 5, 20, 14, 1, false, true, 2023),
			(20230515, '2023/05/15 23:59:59', 2023, 2, 5, 20, 15, 2, true, false, 2023),
			(20230516, '2023/05/16 23:59:59', 2023, 2, 5, 20, 16, 3, true, false, 2023),
			(20230517, '2023/05/17 23:59:59', 2023, 2, 5, 20, 17, 4, true, false, 2023),
			(20230518, '2023/05/18 23:59:59', 2023, 2, 5, 20, 18, 5, true, false, 2023),
			(20230519, '2023/05/19 23:59:59', 2023, 2, 5, 20, 19, 6, true, false, 2023),
			(20230520, '2023/05/20 23:59:59', 2023, 2, 5, 20, 20, 7, false, true, 2023),
			(20230521, '2023/05/21 23:59:59', 2023, 2, 5, 21, 21, 1, false, true, 2023),
			(20230522, '2023/05/22 23:59:59', 2023, 2, 5, 21, 22, 2, true, false, 2023),
			(20230523, '2023/05/23 23:59:59', 2023, 2, 5, 21, 23, 3, true, false, 2023),
			(20230524, '2023/05/24 23:59:59', 2023, 2, 5, 21, 24, 7, true, false, 2023),
			(20230525, '2023/05/25 23:59:59', 2023, 2, 5, 21, 25, 5, true, false, 2023),
			(20230526, '2023/05/26 23:59:59', 2023, 2, 5, 21, 26, 6, true, false, 2023);	                    
        return true;
    end
$function$
;

/* REPORT*/

/*Представление с валидной историей изменений источника) */
--drop view report.View_Valid_card_app_data;

CREATE OR REPLACE view  report.View_Valid_card_app_data as(	
		select id , 	
            	id_client,	
            	status_nm,	
            	create_dttm,		
            	operation_type,	
            	updated_dttm,
            	hash,
				updated_dttm valid_from_dttm,    
				coalesce (max(src.updated_dttm) over ( /* достраиваем дату завершения версии */
	     			partition by src.id
	    			order by src.updated_dttm rows between 1 following and 1 following)- interval '1 second',
	    			TO_TIMESTAMP('2999/12/31 23:59:59', 'YYYY/MM/DD HH24:MI:SS') ) valid_to_dttm, 
				deleted_flg,
				deleted_dttm
		from dwh_ods.card_application_data_hist src);


/* Представление  - Выборка о крайнем состоянии источника */
	
--drop view report.View_Actual_card_application_data;
CREATE OR REPLACE view  report.View_Actual_card_application_data as(		
	select id ,
		id_client,	
       status_nm,	
       create_dttm,			
       updated_dttm
	from (select a.*,
		row_number() over(partition by a.id order by valid_from_dttm desc) as rn
  				from report.View_Valid_card_app_data a) tmp  
  				where rn = 1 and operation_type != 'D') ;

/*Функций для заполнения ВИТРИН*/


/*без использования календаря */
/*Количество задач пришедших в данное состояние в данный день*/
  					 
CREATE OR REPLACE FUNCTION report.tasks_start_in_state_count() 
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
    declare
      var_period_from DATE = '2023-05-01'; /* Начало периода для построения витрины ОТ */
      var_period_to DATE = now(); /* Конец периода для построения витрины ДО */
    begin  
		truncate table report.tasks_start_in_state_count;
        insert into report.tasks_start_in_state_count		
		select cast(valid_from_dttm as date), status_nm, count(id) 
				from report.View_Valid_card_app_data 					
				where (cast(valid_from_dttm as date) >= var_period_from 
	    				and cast(valid_from_dttm as date) <= var_period_to) 
	    				and deleted_flg = 'N' 
	    		group by cast(valid_from_dttm as date), status_nm 
	    		order by cast(valid_from_dttm as date); 
        return true;
    end
$function$
;

/*с календарем */ 
--  Количестов задач находящихся в данном статус в данный день

CREATE OR REPLACE FUNCTION report.tasks_in_state_count() 
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
    declare
      var_period_from DATE = '2023-05-01'; /* Начало периода для построения витрины ОТ */
      var_period_to DATE = now(); /* Конец периода для построения витрины ДО */
    begin  
		truncate table report.tasks_in_state_count;        	
	    /* витрина - количество задач в каждом из состояний*/
		with tabl as
		(
		select C.date_key,	
		C.date_actual, 
		R.status_nm,
		(select count(R1.id) from report.View_Valid_card_app_data R1 
				where  (R1.deleted_flg = 'N') and R1.status_nm = R.status_nm and 
					(cast(R1.valid_from_dttm as date)<=(C.date_actual) 
					and 
					cast(R1.valid_to_dttm as date)>=(C.date_actual)) 
				group by C.date_actual)  as task_count /*order by date(R1.valid_from_dttm)desc limit 1*/ 
		from  (select date_key, date_actual from dwh_ods.Dim_Date where (date_actual >= var_period_from and date_actual <= var_period_to) ) C, 
			  (select status_nm from report.View_Valid_card_app_data group by status_nm  ) R
		order by C.date_actual, R.status_nm
		)		
			
		insert into report.tasks_in_state_count 	
		select * from tabl where (task_count is not null) and (date_actual >= var_period_from and date_actual <= var_period_to);

        return true;
    end
$function$
;
						

/* даты удаления, статус и количество задач каждого статуса*/
				
CREATE OR REPLACE FUNCTION report.deleted_tasks_count() 
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
    declare
      var_period_from DATE = '2023-05-01'; /* Начало периода для построения витрины ОТ */
      var_period_to DATE = now(); /* Конец периода для построения витрины ДО */
    begin  
		truncate table report.deleted_tasks_count;        	
	    /* витрина - количество задач в каждом из состояний*/
		with tabl as (select C.date_key , C.date_actual, status_nm, count(id) as tasks_count 
						from report.View_Valid_card_app_data  R inner join  dwh_ods.Dim_Date C
						on cast(R.updated_dttm as date) = C.date_actual
						where  	R.deleted_flg = 'Y' 
						group by  cast(R.updated_dttm as date), R.status_nm, C.date_key , C.date_actual)	
			
		insert into report.deleted_tasks_count 	
		select * from tabl where (tasks_count is not null) and (date_actual >= var_period_from and date_actual <= var_period_to);

        return true;
    end
$function$
;
 				  
/* Идентификатор задачи, дата удаления, статус*/

CREATE OR REPLACE FUNCTION report.deleted_tasks_in_state() 
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
    declare
      var_period_from DATE = '2023-05-01'; /* Начало периода для построения витрины ОТ */
      var_period_to DATE = now(); /* Конец периода для построения витрины ДО */     
    begin  
		truncate table report.deleted_tasks_in_state;       	
		
		insert into report.deleted_tasks_in_state 	
		select C.date_key , C.date_actual, id , status_nm 
						from report.View_Valid_card_app_data  R inner join  dwh_ods.Dim_Date C
						on cast(R.updated_dttm as date) = C.date_actual
						where  	R.deleted_flg = 'Y' and  (C.date_actual >= var_period_from and C.date_actual <= var_period_to);
        return true;
    end
$function$
;	

/* ------------------------------------------------------------------------------------------------------------------------------*/
/* ТЕСТИРОВАНИЕ*/

/*ЗАПОЛНЕНИЕ ТЕСТОВЫМИ ДАННЫМИ*/
	    			
/* Функция заполнения Источника ТЕСТОВЫМИ данными*/

CREATE OR REPLACE FUNCTION oltp_src_system.load_src_for_test() 
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
    declare
    begin  
		truncate table oltp_src_system.card_application_data;       	
		insert into  oltp_src_system.card_application_data (
			id,
			id_client,
			status_nm,
			create_dttm,
			update_dttm)
		values 
			(1 , 10, 'canseled' ,   '2023-05-02 09:00:00.877 +0300', '2023-05-03 09:00:00.877 +0300' ), -- отменена задача
			(2 , 20, 'closed' ,     '2023-05-02 09:00:00.877 +0300', '2023-05-05 12:00:00.877 +0300' ), -- заявка закрыта
			(4 , 40, 'created' ,    '2023-05-04 11:00:00.877 +0300', '2023-05-04 11:00:00.877 +0300' ), --создана
			(5 , 50, 'canseled' ,   '2023-05-10 09:00:00.877 +0300', '2023-05-11 12:00:00.877 +0300' ), -- отменена задача
			(6 , 60, 'closed' ,     '2023-05-11 09:00:00.877 +0300', '2023-05-16 09:00:00.877 +0300' ),
			(7 , 70, 'closed' ,     '2023-05-15 09:00:00.877 +0300', '2023-05-18 10:00:00.877 +0300' );
        return true;
    end
$function$
;

/* Функция заполнения Таблицы захвата изменений CDC ТЕСТОВЫМИ данными */

CREATE OR REPLACE FUNCTION oltp_cdc_src_system.load_cdc_for_test() 
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
    declare
    begin  
		truncate table oltp_cdc_src_system.cdc_card_application_data_changes;   	
		insert into  oltp_cdc_src_system.cdc_card_application_data_changes ( 
			id,
			id_client ,
			status_nm,
			create_dttm,
			update_dttm,
			operation_type,
			updated_dttm /* время обновления*/
			)
		values 
		(1 , 10, 'created' ,    '2023-05-02 09:00:00.877 +0300', '2023-05-02 09:00:00.877 +0300', 'I', '2023-05-02 09:00:00.877 +0300'  ), --создана задача для клиета 20 
		(1 , 10, 'canseled' ,   '2023-05-02 09:00:00.877 +0300', '2023-05-03 09:00:00.877 +0300', 'U', '2023-05-03 09:00:00.877 +0300'  ), -- отменена задача
		(2 , 20, 'created' ,    '2023-05-02 09:00:00.877 +0300', '2023-05-02 09:00:00.877 +0300', 'I', '2023-05-02 09:00:00.877 +0300'  ), -- создана
		(2 , 20, 'considered',  '2023-05-02 09:00:00.877 +0300', '2023-05-02 12:00:00.877 +0300', 'U', '2023-05-02 12:00:00.877 +0300' ), -- в тот же день перешла на рассмотрение
		(2 , 20, 'approved' ,   '2023-05-02 09:00:00.877 +0300', '2023-05-03 09:00:00.877 +0300', 'U', '2023-05-03 09:00:00.877 +0300' ), --одобрена
		(2 , 20, 'terminated' , '2023-05-02 09:00:00.877 +0300', '2023-05-05 09:00:00.877 +0300' , 'U', '2023-05-05 09:00:00.877 +0300'), --выполнена (выдана карта например)
		(2 , 20, 'closed' ,     '2023-05-02 09:00:00.877 +0300', '2023-05-05 12:00:00.877 +0300', 'U', '2023-05-05 12:00:00.877 +0300' ), -- заявка закрыта
		(3 , 30, 'created' ,    '2023-05-03 09:00:00.877 +0300', '2023-05-03 09:00:00.877 +0300', 'I', '2023-05-03 09:00:00.877 +0300'  ), --создана
		(3 , 30, 'considered' , '2023-05-03 09:00:00.877 +0300', '2023-05-04 09:00:00.877 +0300', 'U', '2023-05-04 09:00:00.877 +0300' ), -- начато рассмотрение
		(3 , 30, 'rejected' ,   '2023-05-03 09:00:00.877 +0300', '2023-05-05 09:00:00.877 +0300', 'U', '2023-05-05 09:00:00.877 +0300' ), --отклонена
		(3 , 30, 'closed' ,     '2023-05-03 09:00:00.877 +0300', '2023-05-06 09:00:00.877 +0300', 'U', '2023-05-06 09:00:00.877 +0300' ),--закрыта заявка
		(3 , 30, 'closed' ,     '2023-05-03 09:00:00.877 +0300', '2023-05-07 09:00:00.877 +0300', 'D', '2023-05-07 09:00:00.877 +0300' ),--задача удалена
		(4 , 40, 'created' ,    '2023-05-04 11:00:00.877 +0300', '2023-05-04 11:00:00.877 +0300', 'I', '2023-05-04 11:00:00.877 +0300'  ), --создана
		(5 , 50, 'created' ,    '2023-05-10 09:00:00.877 +0300', '2023-05-10 10:00:00.877 +0300', 'I', '2023-05-10 10:00:00.877 +0300'  ), --создана задача для клиета 20 
		(5 , 50, 'canseled' ,   '2023-05-10 09:00:00.877 +0300', '2023-05-11 12:00:00.877 +0300' , 'U', '2023-05-11 12:00:00.877 +0300'), -- отменена задача
		(6 , 60, 'created' ,    '2023-05-11 09:00:00.877 +0300', '2023-05-11 09:00:00.877 +0300', 'I', '2023-05-11 09:00:00.877 +0300'  ),
		(6 , 60, 'considered' , '2023-05-11 09:00:00.877 +0300', '2023-05-12 09:00:00.877 +0300' , 'U', '2023-05-12 09:00:00.877 +0300'),
		(6 , 60, 'rejected' ,   '2023-05-11 09:00:00.877 +0300', '2023-05-15 09:00:00.877 +0300' , 'U', '2023-05-15 09:00:00.877 +0300'),
		(6 , 60, 'closed' ,     '2023-05-11 09:00:00.877 +0300', '2023-05-16 09:00:00.877 +0300' , 'U', '2023-05-16 09:00:00.877 +0300'),
		(7 , 70, 'created' ,    '2023-05-15 09:00:00.877 +0300', '2023-05-15 14:00:00.877 +0300', 'I', '2023-05-15 14:00:00.877 +0300'  ),--создана
		(7 , 70, 'considered' , '2023-05-15 09:00:00.877 +0300', '2023-05-15 16:00:00.877 +0300' , 'U', '2023-05-15 16:00:00.877 +0300'),--на рассмотрении
		(7 , 70, 'paused' ,     '2023-05-15 09:00:00.877 +0300', '2023-05-15 20:00:00.877 +0300' , 'U', '2023-05-15 20:00:00.877 +0300'),--на паузе
		(7 , 70, 'considered' , '2023-05-15 09:00:00.877 +0300', '2023-05-16 10:00:00.877 +0300' , 'U', '2023-05-16 10:00:00.877 +0300'),--на рассмотрении
		(7 , 70, 'approved' ,   '2023-05-15 09:00:00.877 +0300', '2023-05-17 10:00:00.877 +0300' , 'U', '2023-05-17 10:00:00.877 +0300'),-- одобрена
		(7 , 70, 'terminated' , '2023-05-15 09:00:00.877 +0300', '2023-05-17 15:00:00.877 +0300' , 'U', '2023-05-17 15:00:00.877 +0300'), --выполнена (выдана карта например)
		(7 , 70, 'closed' ,     '2023-05-15 09:00:00.877 +0300', '2023-05-18 10:00:00.877 +0300' , 'U', '2023-05-18 10:00:00.877 +0300'), --закрыта
		(8 , 80, 'created' ,     '2023-05-15 10:00:00.877 +0300', '2023-05-15 10:00:00.877 +0300', 'I', '2023-05-15 10:00:00.877 +0300'  ),-- создана
		(8 , 80, 'created' ,     '2023-05-15 10:00:00.877 +0300', '2023-05-16 11:00:00.877 +0300', 'D', '2023-05-16 11:00:00.877 +0300'  ), --удалили
		(9 , 90, 'created' ,     '2023-05-15 10:00:00.877 +0300', '2023-05-15 10:00:00.877 +0300', 'I', '2023-05-15 10:00:00.877 +0300'  ),-- создана
		(9 , 90, 'created' ,     '2023-05-15 10:00:00.877 +0300', '2023-05-16 12:00:00.877 +0300' , 'D', '2023-05-16 12:00:00.877 +0300'),--удалена
		(10 , 170, 'created' ,    '2023-05-15 09:00:00.877 +0300', '2023-05-15 14:00:00.877 +0300', 'I', '2023-05-15 14:00:00.877 +0300' ),--создана
		(10 , 170, 'considered' , '2023-05-15 09:00:00.877 +0300', '2023-05-15 16:00:00.877 +0300' , 'U', '2023-05-15 16:00:00.877 +0300'),--на рассмотении
		(10 , 170, 'considered' , '2023-05-15 09:00:00.877 +0300', '2023-05-16 20:00:00.877 +0300' , 'D', '2023-05-16 20:00:00.877 +0300');--удалена
        return true;
    end
$function$
;


/*--------------------------------------------------------------------------------------------------------------*/   
/* Заполнение календаря - пока сделано только на май и без функций генерации */
truncate table dwh_ods.Dim_Date;
select dwh_ods.create_Dim_Date();
select * from dwh_ods.Dim_Date; 


/*ПОСЛЕДОВАТЕЛЬНОСТЬ ТЕСТА*/

    /* ШАГ 1 Подготовка пустого хранилища */
truncate table dwh_stage.card_application_data_dwh_src;
truncate table dwh_ods.card_application_data_hist;

/* ШАГ 2 ЗАПОЛНЕНИЕ ИСТОЧНИКА и CDC ДЛЯ ТЕСТИРОВАНИЯ*/
select oltp_src_system.load_src_for_test(); /* Обнуление и Загрузка источника тестовыми данными*/
 
/* Просмотр*/ select * from oltp_src_system.card_application_data cad; /* Источник*/

select oltp_cdc_src_system.load_cdc_for_test(); /* Обнуление и  Загрузка CDC историей изменения тестовых данных */

/* Просмотр*/ select * from oltp_cdc_src_system.cdc_card_application_data_changes; /*CDC */

	
/*  ШАГ 3 Наполнение Stage хранилища*/

select dwh_stage.load_from_cdc_src_system_card_application_data_to_stage(); /* переливаем из cdc в STAGE  хранилища  */

/* Просмотр*/ select * from dwh_stage.card_application_data_dwh_src; /* dwh stage*/

/* ШАГ 4 Наполнение ODS хранилища*/
select dwh_ods.load_card_application_data_hist(); /* заполняем хранилище, детальный слой (даты валидности заполняются на момент загрузки)  */

/*Просмотр*/ select * from dwh_ods.card_application_data_hist order by create_dttm; /* dwh ods*/


/*просмотр всех слоев */
select * from oltp_src_system.card_application_data cad; /* Источник*/
select * from oltp_cdc_src_system.cdc_card_application_data_changes; /*CDC */
select * from dwh_stage.card_application_data_dwh_src; /* dwh stage*/
select * from dwh_ods.card_application_data_hist order by create_dttm; /* dwh ods*/

/* ШАГ 5 Просмотр результатов постороения представления с валидными датами */
select * from report.View_Valid_card_app_data; /* Представление с валидной ИСТОРИЕЙ изменений источника) */


/* ШАГ 6 */

/* Задание А. Состояние источника на момент последнего сохранения в ods */
select * from report.View_Actual_card_application_data; /* Соответствует ИСТОЧНИКУ на момент послоедней выгрузки*/

/*Задание B. Витрины */ 
select report.tasks_start_in_state_count(); /* Функция генерации витрины - Количество задач, получивших данный статус в данный день*/
select report.tasks_in_state_count();       /* Функция генерации витрины - Количество задач, находящихся в данном статус в данный день */
select report.deleted_tasks_count(); /* Функция генерации витрины - Количество удаленных задач, находящихся в данном статусе в данный день*/
select report.deleted_tasks_in_state(); /*Функция генерации витрины - Задачи, удаленные в данном статусе в данный день */

/* Просмотр отчетов*/
select * from report.tasks_start_in_state_count; /* ВИТРИНА -Количество задач, получивших данный статус в данный день */
select * from report.tasks_in_state_count;       /* ВИТРИНА - Количество задач, находящихся в данном статус в данный день*/
select * from report.deleted_tasks_count; /* ВИТРИНА - Количество удаленных задач, находящихся в данном статусе в данный день*/
select * from report.deleted_tasks_in_state; /* ВИТРИНА - Задачи, удаленные в данном статусе в данный день*/


/* ШАГ 7 - либо переход в Airflow*/
/*Функции  РАНДОМ-генерации данных для тестирования*/
select oltp_src_system.create_tasks(); /* генерация новых задач*/
select oltp_src_system.update_existed_task(); /* изменение случайных существущих*/
select oltp_src_system.delete_existed_task(); /* удаление случайных задач*/
