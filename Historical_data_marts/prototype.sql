/*
	DFCT_PHONE - исторические данные телефонов клиентов-физлиц
*/

/* Вариант с построение fdtd не удался, запуталась. Поэтому сдала то, что более менее-приближается к результату
 * Предполагаю(!!), что для построения варианта с корзинкой необходимо было  
 * 1. сформировать таблицу с клиентами ФЛ 
 * 2. сформировать таблицу с перекодированными локальными ключами
 * 3. сформировать таблицу с данными путем "наложения маски" ФЛ на таблицу с контактами (с новыми ключами)
 * 4. Далее необходимо собрать корзинку изменения версий по датам  в рамках mdm_customer_rk и phone_number (учитываем, что у одного клиента много телефонов, 
 * и у одного телефона много клиентов - а даты должны быть разнесены на периоды 
 * 5. Рассчитать и установить флаги
 * 6. Проверить и откорректировать периоды "схлопывая даты для одного номера телефона одного клиента"*/

/* Вариант для сдачи выполнен более последовательно, но в правильном итоге должно быть почти в два раза больше строк за счет разнесения периодов*/

/* Из таблицы counterparty берем только данные о физлицах*/
drop table if exists p_dfct_phone_0010;
create table p_dfct_phone_0010 as
select 
	c_.counterparty_rk as counterparty_rk, 
	c_.effective_from_date effective_from_date , 
	c_.effective_to_date effective_to_date
	from counterparty c_ 
	inner join dict_counterparty_type_cd dctc_ 
	on c_.counterparty_type_cd = dctc_.counterparty_type_cd 
	where dctc_.counterparty_type_desc  = 'физическое лицо' /* выбираем только клиентов физлиц*/
		  and c_.src_cd  = 'MDMP' and dctc_.src_cd  = 'MDMP'/* отбираем данные только из унифицированной системы MDM, имеющей код MDMP*/ 
		  /*возможно лучше еще добавить проверку соответствия дат Справочника и дат актуальности среза таблицы counterparty */
	; 


/* Вполняем замену локальных ключей на глобальные, учитываем время действия соответствия ключей */
drop table if exists p_dfct_phone_0020;
create table p_dfct_phone_0020 as 
with 
/* Отбираем контакты, источник которых не mdmp и сопоставляем  conterparty_rk с уникальным ключом uniq_counterparty_rk и датами его актуальности*/
p_dfct_phone_0020_1 as (select  cc_.counterparty_rk , /* локальный ключ таблицы Контактов  */
		cxuc_.uniq_counterparty_rk as mdm_customer_rk, /*Глобальный ключ*/
		/*проверяем чтобы даты соответствовали датам версии mdm_counterparty_rk для counterparty_rk
		 * Проверяем различные варианты пересечения даты Начала и окончания версии из таблицы Контакты с датами версий из таблиц Связей*/
		case when cc_.effective_from_date >= cxuc_.effective_from_date and cc_.effective_from_date <= cxuc_.effective_to_date then cc_.effective_from_date
			when cc_.effective_from_date < cxuc_.effective_from_date and  cc_.effective_to_date > cxuc_.effective_from_date then cxuc_.effective_from_date
			else null 
		end as effective_from_date,		
		case when cc_.effective_to_date >= cxuc_.effective_from_date and cc_.effective_to_date <= cxuc_.effective_to_date then cc_.effective_to_date
			when cc_.effective_to_date >= cxuc_.effective_from_date and cc_.effective_to_date > cxuc_.effective_to_date then cxuc_.effective_to_date
			else null 
		end as  effective_to_date,		
		cc_.contact_desc as phone_num, 
		cc_.contact_type_cd as phone_type_cd, 
		cc_.contact_quality_code, 
		cc_.trust_system_flg as trust_system_flg, 
		cc_.src_cd as src_code
		from counterparty_contact cc_  /* Таблица контактов */
	inner join counterparty_x_uniq_counterparty cxuc_ /* Таблица связей*/
	on (cc_.counterparty_rk = cxuc_.counterparty_rk) and (cc_.effective_from_date < cxuc_.effective_to_date ) /*в расчеты не должны попасть версии контактов появившиеся после актуальности связи ключей   */
	where cc_.src_cd <> 'MDMP' and cxuc_.src_cd  = 'MDMP'
) ,
/* Отбираем контакты клиентов, полученные из источника mdmp, а значит их counterparty_rk является 
 * глобальным ключом uniq_counterparty_rk и не нужно проводить преобразования */
p_dfct_phone_0020_2 as (select  cc_.counterparty_rk , /*Техническое поле, чтобы проводить в дальнейшем слияние двух выборок*/
		cc_.counterparty_rk as mdm_customer_rk, /*глобальный ключ MDMP */
		cc_.effective_from_date, 
		cc_.effective_to_date, 	
		cc_.contact_desc as phone_num, 
		cc_.contact_type_cd as phone_type_cd, 
		cc_.contact_quality_code, 
		cc_.trust_system_flg as trust_system_flg, 
		cc_.src_cd as src_code
		from counterparty_contact cc_  where cc_.src_cd = 'MDMP'
)
/* Объединяем две выборки*/
(select * from p_dfct_phone_0020_1 where (effective_from_date is not null or effective_to_date is not null)) /*отсекаем строки с невалидными датами */
	union 
(select * from p_dfct_phone_0020_2);

/* Выполняем соединение с таблицей p_dfct_phone_0010, чтобы отбирать контакты только физлиц */ 
drop table if exists p_dfct_phone_0030; /* Контакты только для ФЛ*/
create table p_dfct_phone_0030 as
select 
	cc_cxuc_.mdm_customer_rk,
	case when cc_cxuc_.effective_from_date >= c_fl_.effective_from_date and cc_cxuc_.effective_from_date <= c_fl_.effective_to_date then cc_cxuc_.effective_from_date
		when cc_cxuc_.effective_from_date < c_fl_.effective_from_date and  cc_cxuc_.effective_to_date > c_fl_.effective_from_date then c_fl_.effective_from_date	
		else null 
		end as effective_from_date,		
	case when cc_cxuc_.effective_to_date >= c_fl_.effective_from_date and cc_cxuc_.effective_to_date <= c_fl_.effective_to_date then cc_cxuc_.effective_to_date
		when cc_cxuc_.effective_to_date >= c_fl_.effective_from_date and cc_cxuc_.effective_to_date > c_fl_.effective_to_date then c_fl_.effective_to_date
		else null 
		end as  effective_to_date,	
	cc_cxuc_.phone_num,
	cc_cxuc_.phone_type_cd,
	cc_cxuc_.contact_quality_code,
	cc_cxuc_.trust_system_flg,
	cc_cxuc_.src_code
from p_dfct_phone_0020 cc_cxuc_
/* соединяемся ФЛ */
inner join p_dfct_phone_0010 c_fl_  
	on cc_cxuc_.mdm_customer_rk = c_fl_.counterparty_rk;  

/* Для соблюдения историчности "пересечения" телефонов*/
/* В рамках задачи не доделано - есть случаи когда у одног клиента не разбиты периоды владения несколькими телефонами*/
drop table if exists p_dfct_phone_0040;
create table p_dfct_phone_0040 as 
select 
src_1_.mdm_customer_rk, 
src_2_.effective_from_date as business_start_dt, 
/* достраиваем дату завершения версии 
 * Сравниваем дату окончания версии из второй таблицы с датой начала версий для следующей строки*/
coalesce(min(src_2_.effective_from_date) over ( 
		partition by src_1_.phone_num, src_1_.mdm_customer_rk
		order by src_2_.effective_from_date  
		rows between 1 following and 1 following), src_1_.effective_to_date) as business_end_dt,
src_1_.phone_num ,
src_1_.phone_type_cd , 
src_1_.contact_quality_code , 
src_1_.trust_system_flg , 
src_1_.src_code
from p_dfct_phone_0030  src_1_ 
inner join 
p_dfct_phone_0030   src_2_
 on src_1_.phone_num = src_2_.phone_num and (src_1_.effective_from_date is not null or src_1_.effective_from_date is not null) 
 										and  (src_1_.effective_from_date <= src_2_.effective_from_date  
 												and src_1_.effective_to_date >= src_2_.effective_to_date) 
order by src_1_.effective_from_date;
 

/* Построение новых полей с флагами*/
drop table if exists p_dfct_phone_0050;
create table p_dfct_phone_0050 as 
with p_dfct_phone_0050_1 as (select
	src_.mdm_customer_rk,
	src_.phone_type_cd ,
	src_.business_start_dt, 
	src_.business_end_dt, 
	src_.phone_num,
	src_.src_code,
	src_.contact_quality_code,
	case src_.phone_type_cd  when 'NotificPhone' then true else false end as notification_flg,	
	case src_.phone_type_cd  when 'ATMPhone' then true else false end as atm_flg,
	src_.trust_system_flg,
	/* Если номер телефона в дату начала версии указан более, чем у  одного клинета, значит этот телефон является дублирующимся
	 * 
	 *В РАМКАХ ЗАДАЧИ ПРОВЕРКА НЕ ИДЕАЛЬНА! 
	 * Необходима корректировка, поскольку в итоговый набор попадают данные клиентов с единственным номером 
	 * на начало версии, если на эту дату есть и дубли  других клиентов*/
	case when count(src_.mdm_customer_rk) over (partition by src_.phone_num, src_.business_start_dt 
												order by src_.business_start_dt) >1 	
		then true 
		else false 
		end as duplication_flg,

	/*Для дальнейшей приоритеризации закодируем все необходимые атрибуты в порядке приоритета по ТЗ
	 * На следующем шаге будем строить таблицу приоритетов  */
	case when (upper(src_.contact_quality_code)  like '%GOOD%') 
		then 1 
		else 2 
		end  as contact_quality_code_num, 
	case when src_.src_code = 'MDMP' then 1
		when src_.src_code = 'WAYN' then 2
		when src_.src_code = 'RTLL' then 3
		when src_.src_code = 'RTLS' then 4
		when src_.src_code = 'CFTB' then 5
	end as src_code_num,
	case src_.phone_type_cd when 'NotificPhone' then 1
							when 'ATMPhone' then 2
							when 'MobilePersonalPhone' then 3
							when 'MobileWorkNumber' then 4
							when 'HomePhone' then 5
	end as phone_type_cd_num,	
	case src_.trust_system_flg when true then 1
							when false then 2
	end as trust_system_num
from p_dfct_phone_0040 src_ ),

/*Установка флага Лучшего телефона для клиента main_phone_flg*/ 

/* В рамках задачи существуют случаи некорректной установки флага!!! */

p_dfct_phone_0050_2 as (select *,
/* создаем таблицу приоритетов и отбираем только самую верхнюю строку , для которой установим флаг*/
	case when (row_number() over (partition by src_.mdm_customer_rk, src_.business_start_dt 
						order by   src_.trust_system_num, src_.contact_quality_code_num, src_.src_code_num, src_.phone_type_cd_num, src_.business_start_dt desc)) = 1 
	then true 
	else false 
	end as  main_phone_flg
from  p_dfct_phone_0050_1 src_ ),
/* расчет и установку флага main_dup_flg */
p_dfct_phone_0050_3 as (
select *,
/* создаем таблицу приоритетов и отбираем только самую верхнюю строку , для которой установим флаг*/
case when (row_number() over (partition by src_.phone_num, src_.business_start_dt 
							order by   src_.trust_system_num, src_.contact_quality_code_num, src_.src_code_num, src_.phone_type_cd_num, src_.business_start_dt desc)) = 1 
	then true 
	else false 
end as  main_dup_flg
from  p_dfct_phone_0050_2 src_ 
where duplication_flg
)
/* Собираем информацию о телефонах без дублей и с дублями  */
/* Поскольку строкам с дублированными телефонами создали и установили флаг, необходимо предусмотреть это при слиянии.
 * Для телефонов без дублей флаг основного клиента для номера устанавливаем в true*/
(select *, true as main_dup_flg from p_dfct_phone_0050_2 where not duplication_flg) 
union
(select * from p_dfct_phone_0050_3 ) ;

/* Кроме указанных недоделок. Осталось несколько строк, которые нужно было бы схлопнуть в один период, но не доделано*/

/* Нужные атрибуты перемещаем в dfct_phone */ 
drop table if exists p_dfct_phone;
create table p_dfct_phone as (
select 
	src_.mdm_customer_rk,
	src_.phone_type_cd ,
	src_.business_start_dt::timestamp(0),
	src_.business_end_dt::timestamp(0) ,
	src_.phone_num,
	src_.notification_flg,	
	src_.atm_flg,
	src_.trust_system_flg,
	src_.duplication_flg,
	src_.main_dup_flg,
	src_.main_phone_flg
from p_dfct_phone_0050 src_);

select * from p_dfct_phone order by mdm_customer_rk, business_start_dt , phone_num, phone_type_cd ;


