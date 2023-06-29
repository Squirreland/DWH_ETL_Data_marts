/* NULL в PK
 * Результат 0*/
select count(1) from p_dfct_phone where mdm_customer_rk is null;
select count(1) from p_dfct_phone where phone_type_cd is null;
select count(1) from p_dfct_phone where phone_num is null;
select count(1) from p_dfct_phone where business_start_dt is null;

/* -1 в PK
 * Результат 0 */
select count(1) from p_dfct_phone where mdm_customer_rk = -1;
select count(1) from p_dfct_phone where phone_type_cd = -1;
select count(1) from p_dfct_phone where phone_num = -1;
select count(1) from p_dfct_phone where business_start_dt = -1;

/* Дубли по PK
 * Результат пусто*/
select mdm_customer_rk, business_start_dt, phone_type_cd, phone_num from p_dfct_phone group by 1, 2, 3, 4 having count(1) >1

/* Заполненость атрибута 
 * в рамках задачи все атрибуты либо ключи (кроме business_end_dt, которая рассчитывается) , либо флаги (значит null не можем проверить) */

/* Пересечение по истории - проверить нельзя - оно у нас естественно есть*/

/* Дырки в истории - тоже не проверям */

/* Неправильная дата завершения в будущем  
 * Результат 0  */
select count(1) from p_dfct_phone where business_start_dt> now() and business_end_dt <> date'2999-12-31'

/* Версии из будущего
 * Результат 0*/
select count(1) from p_dfct_phone where business_start_dt > now()


/* Проверки бизнес-состояния*/

/* Некорректно установлены даты версий:
 * Проверка , что нет даты конца версии больше даты начала версии 
 * (теоретически могло возникнуть при пересчете дат)
 * Результат 0*/
select count(1) from p_dfct_phone where business_start_dt > business_end_dt 

/* Мертвые души:
 * В прототипе не должны присутсвтовать клиенты, которых нет в таблице источнике counterparty
 * Результат 0*/
select  count(*) from p_dfct_phone where mdm_customer_rk not in (select distinct counterparty_rk from counterparty)

/* Клиенты ФЛ:
 * Проверить по справочнику и таблице источнику counterparty, что отобранные контакты принадлежат  клиентам, которые являются физлицами
 * Результат 0*/
select  count(*) from p_dfct_phone where mdm_customer_rk not in 
		(select distinct c_.counterparty_rk 
			from counterparty c_ inner join dict_counterparty_type_cd dctc_ 
				on c_.counterparty_type_cd = dctc_.counterparty_type_cd 
			where dctc_.counterparty_type_desc  = 'физическое лицо' )

/* Флаг основного клиента:
 * Если телефон принадлежит только одному клиенту, то должен быть установлен флаг основного клиента для этого телефона duplication_flg.
 * Результат 0*/
with phone_number_count as(
select count(mdm_customer_rk) as cnt, duplication_flg, business_start_dt  
	from p_dfct_phone  
	group by phone_num, 2,3 having duplication_flg  = true )
select count(1) from phone_number_count where cnt = 1


/* Флаг лучшего телефона:
 * Если у клиента только один телефон в период времени,  то его флаг лучшего телефона для клиента main_phone_flg должен быть установлен true 
 * Результат  : в случае правильно построенного прототипа - 0 
 * В моем прототипе данные недоделаные результат - 11  */
with phone_number_count as(
select count(mdm_customer_rk) as cnt, duplication_flg, main_phone_flg, business_start_dt 
	from p_dfct_phone group by phone_num, 2, 3, 4  having  duplication_flg  = true order by main_phone_flg )
select count(*) from phone_number_count where cnt = 1 and main_phone_flg







 
 

