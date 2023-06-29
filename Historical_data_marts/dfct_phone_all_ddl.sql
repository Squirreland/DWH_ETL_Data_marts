create table counterparty (
	counterparty_rk	int8,
	effective_from_date date,
	effective_to_date date,
	counterparty_type_cd text,
	src_cd text,
);

create table dict_counterparty_type_cd (
	counterparty_type_cd text,
	effective_from_date date,
	effective_to_date date,
	counterparty_type_desc text,
	src_cd text
);

create table counterparty_x_uniq_counterparty (
	counterparty_rk int8,
	effective_from_date date,
	effective_to_date date,
	uniq_counterparty_rk int8,
	src_cd text
);

create table counterparty_contact (
	counterparty_rk int8,
	effective_from_date date,
	effective_to_date date,
	contact_desc text,
	contact_type_cd text,
	contact_quality_code text,
	trust_system_flg bool,
	src_cd text
);

create table dfct_phone ( 
	mdm_customer_rk int8,
	phone_type_cd text,
	business_start_dt date,
	business_end_dt date,
	phone_num text,
	notification_flg bool,
	atm_flg bool,
	trust_system_flg bool,
	duplication_flg bool,
	main_dup_flg bool,
	main_phone_flg bool
);


insert into counterparty values (1001,'2017-08-12','2018-03-24','legal','MDMP');
insert into counterparty values (1001,'2018-03-24','2999-12-31','2','MDMP');
insert into counterparty values (1003,'2018-08-28','2019-03-15','natural','MDMP');
insert into counterparty values (1003,'2019-03-15','2999-12-31','1','MDMP');
insert into counterparty values (1005,'2012-11-01','2013-06-13','1','MDMP');
insert into counterparty values (1005,'2013-06-13','2999-12-31','1','MDMP');
insert into counterparty values (1007,'2014-08-16','2014-11-23','natural','MDMP');
insert into counterparty values (1007,'2014-11-23','2999-12-31','1','MDMP');
insert into counterparty values (1009,'2012-02-01','2012-07-26','legal','MDMP');
insert into counterparty values (1009,'2012-07-26','2999-12-31','1','MDMP');
insert into counterparty values (1015,'2018-04-21','2018-06-17','1','MDMP');
insert into counterparty values (1015,'2018-06-17','2999-12-31','1','MDMP');
insert into counterparty values (1021,'2016-08-25','2016-09-11','natural','MDMP');
insert into counterparty values (1021,'2016-09-11','2999-12-31','1','MDMP');
insert into counterparty values (1023,'2013-03-05','2013-08-18','1','MDMP');
insert into counterparty values (1023,'2013-08-18','2999-12-31','natural','MDMP');
insert into counterparty values (1025,'2011-12-31','2012-05-28','1','MDMP');
insert into counterparty values (1025,'2012-05-28','2999-12-31','1','MDMP');
insert into counterparty values (1027,'2013-10-13','2014-03-04','natural','MDMP');
insert into counterparty values (1027,'2014-03-04','2999-12-31','1','MDMP');
insert into counterparty values (1029,'2018-09-22','2019-01-23','legal','MDMP');
insert into counterparty values (1029,'2019-01-23','2999-12-31','2','MDMP');
insert into counterparty values (1031,'2012-01-04','2012-03-17','1','MDMP');
insert into counterparty values (1031,'2012-03-17','2999-12-31','1','MDMP');
insert into counterparty values (1033,'2015-11-21','2016-03-30','natural','MDMP');
insert into counterparty values (1033,'2016-03-30','2999-12-31','1','MDMP');
insert into counterparty values (1035,'2012-11-08','2013-03-21','legal','MDMP');
insert into counterparty values (1035,'2013-03-21','2999-12-31','1','MDMP');
insert into counterparty values (1037,'2019-06-05','2020-03-30','natural','MDMP');
insert into counterparty values (1037,'2020-03-30','2999-12-31','1','MDMP');
insert into counterparty values (1041,'2015-09-06','2016-04-08','1','MDMP');
insert into counterparty values (1041,'2016-04-08','2999-12-31','1','MDMP');
insert into counterparty values (1043,'2019-01-09','2019-10-27','natural','MDMP');
insert into counterparty values (1043,'2019-10-27','2999-12-31','1','MDMP');
insert into counterparty values (1045,'2015-05-16','2016-03-22','1','MDMP');
insert into counterparty values (1045,'2016-03-22','2999-12-31','1','MDMP');
insert into counterparty values (1051,'2019-07-25','2019-10-27','natural','MDMP');
insert into counterparty values (1051,'2019-10-27','2999-12-31','1','MDMP');
insert into counterparty values (1053,'2019-11-19','2020-09-13','1','MDMP');
insert into counterparty values (1053,'2020-09-13','2999-12-31','natural','MDMP');
insert into counterparty values (1055,'2019-12-24','2020-08-28','legal','MDMP');
insert into counterparty values (1055,'2020-08-28','2999-12-31','2','MDMP');
insert into counterparty values (1057,'2014-05-09','2014-09-25','natural','MDMP');
insert into counterparty values (1057,'2014-09-25','2999-12-31','1','MDMP');
insert into counterparty values (1059,'2013-02-07','2013-12-05','1','MDMP');
insert into counterparty values (1059,'2013-12-05','2999-12-31','natural','MDMP');
insert into counterparty values (1061,'2011-03-30','2011-05-21','natural','MDMP');
insert into counterparty values (1061,'2011-05-21','2999-12-31','1','MDMP');
insert into counterparty values (1063,'2018-04-19','2018-09-30','natural','MDMP');
insert into counterparty values (1063,'2018-09-30','2999-12-31','1','MDMP');
insert into counterparty values (1065,'2013-12-29','2014-03-15','1','MDMP');
insert into counterparty values (1065,'2014-03-15','2999-12-31','natural','MDMP');
insert into counterparty values (1067,'2012-08-06','2013-04-09','1','MDMP');
insert into counterparty values (1067,'2013-04-09','2999-12-31','natural','MDMP');
insert into counterparty values (1069,'2011-10-17','2012-07-31','natural','MDMP');
insert into counterparty values (1069,'2012-07-31','2999-12-31','1','MDMP');
insert into counterparty values (1075,'2020-04-09','2020-09-17','natural','MDMP');
insert into counterparty values (1075,'2020-09-17','2999-12-31','1','MDMP');
insert into counterparty values (1077,'2012-12-30','2013-06-06','1','MDMP');
insert into counterparty values (1077,'2013-06-06','2999-12-31','natural','MDMP');
insert into counterparty values (1079,'2011-05-20','2011-11-02','legal','MDMP');
insert into counterparty values (1079,'2011-11-02','2999-12-31','1','MDMP');
insert into counterparty values (1081,'2018-03-05','2018-06-02','1','MDMP');
insert into counterparty values (1081,'2018-06-02','2999-12-31','natural','MDMP');
insert into counterparty values (1002,'2018-05-25','2999-12-31','1','MDMP');
insert into counterparty values (1004,'2012-06-07','2999-12-31','natural','MDMP');
insert into counterparty values (1006,'2019-08-08','2999-12-31','1','MDMP');
insert into counterparty values (1008,'2013-04-27','2999-12-31','natural','MDMP');
insert into counterparty values (1010,'2019-03-15','2999-12-31','1','MDMP');
insert into counterparty values (1012,'2016-05-08','2999-12-31','natural','MDMP');
insert into counterparty values (1014,'2019-06-30','2999-12-31','1','MDMP');
insert into counterparty values (1016,'2015-10-12','2999-12-31','natural','MDMP');
insert into counterparty values (1018,'2012-04-09','2999-12-31','natural','MDMP');
insert into counterparty values (1020,'2019-05-29','2999-12-31','natural','MDMP');
insert into counterparty values (1022,'2016-01-25','2999-12-31','2','MDMP');
insert into counterparty values (1024,'2012-10-31','2999-12-31','1','MDMP');
insert into counterparty values (1026,'2011-05-22','2999-12-31','natural','MDMP');
insert into counterparty values (1028,'2016-01-15','2999-12-31','1','MDMP');
insert into counterparty values (1030,'2016-03-21','2999-12-31','natural','MDMP');
insert into counterparty values (1032,'2019-02-02','2999-12-31','natural','MDMP');
insert into counterparty values (1034,'2016-12-05','2999-12-31','1','MDMP');
insert into counterparty values (1036,'2011-06-07','2999-12-31','natural','MDMP');
insert into counterparty values (1038,'2015-07-03','2999-12-31','natural','MDMP');
insert into counterparty values (1040,'2015-10-16','2999-12-31','natural','MDMP');
insert into counterparty values (1042,'2018-01-19','2999-12-31','natural','MDMP');
insert into counterparty values (1044,'2013-11-11','2999-12-31','1','MDMP');
insert into counterparty values (1046,'2020-07-02','2999-12-31','natural','MDMP');
insert into counterparty values (1048,'2011-05-17','2999-12-31','1','MDMP');
insert into counterparty values (1050,'2015-01-28','2999-12-31','natural','MDMP');
insert into counterparty values (1052,'2014-04-28','2999-12-31','natural','MDMP');
insert into counterparty values (1054,'2015-11-18','2999-12-31','natural','MDMP');
insert into counterparty values (1056,'2012-09-11','2999-12-31','1','MDMP');
insert into counterparty values (1058,'2017-09-21','2999-12-31','natural','MDMP');
insert into counterparty values (1060,'2020-07-06','2999-12-31','natural','MDMP');
insert into counterparty values (1062,'2019-12-26','2999-12-31','natural','MDMP');
insert into counterparty values (1066,'2011-03-19','2999-12-31','1','MDMP');
insert into counterparty values (1068,'2016-10-23','2999-12-31','legal','MDMP');
insert into counterparty values (1070,'2020-01-16','2999-12-31','2','MDMP');
insert into counterparty values (1072,'2016-10-01','2999-12-31','1','MDMP');
insert into counterparty values (1074,'2013-08-23','2999-12-31','natural','MDMP');
insert into counterparty values (1076,'2014-06-13','2999-12-31','natural','MDMP');
insert into counterparty values (1078,'2012-10-07','2999-12-31','natural','MDMP');
insert into counterparty values (1080,'2015-03-11','2999-12-31','1','MDMP');
insert into counterparty values (1082,'2014-09-07','2999-12-31','1','MDMP');
insert into counterparty values (1084,'2020-08-23','2999-12-31','natural','MDMP');
insert into counterparty values (1086,'2015-07-12','2999-12-31','1','MDMP');
insert into counterparty values (1090,'2012-02-16','2999-12-31','legal','MDMP');
insert into counterparty values (1091,'2019-12-26','2999-12-31','2','MDMP');
insert into counterparty values (1092,'2012-03-10','2999-12-31','1','MDMP');
insert into counterparty values (1093,'2013-05-27','2999-12-31','natural','MDMP');



insert into dict_counterparty_type_cd values ('1','1900-01-01','2999-12-31','физическое лицо','MDMP');
insert into dict_counterparty_type_cd values ('2','1900-01-01','2999-12-31','юридическое лицо','MDMP');
insert into dict_counterparty_type_cd values ('3','1900-01-01','2999-12-31','банк','MDMP');
insert into dict_counterparty_type_cd values ('legal','1900-01-01','2999-12-31','юридическое лицо','MDMP');
insert into dict_counterparty_type_cd values ('natural','1900-01-01','2999-12-31','физическое лицо','MDMP');


insert into counterparty_x_uniq_counterparty values (1011,'2020-01-31','2020-11-11',1058,'MDMP');
insert into counterparty_x_uniq_counterparty values (1011,'2020-11-11','2999-12-31',1003,'MDMP');
insert into counterparty_x_uniq_counterparty values (1095,'2015-04-04','2015-10-10',1020,'MDMP');
insert into counterparty_x_uniq_counterparty values (1095,'2015-10-10','2999-12-31',1068,'MDMP');
insert into counterparty_x_uniq_counterparty values (1071,'2019-09-17','2019-12-14',1063,'MDMP');
insert into counterparty_x_uniq_counterparty values (1071,'2019-12-14','2999-12-31',1046,'MDMP');
insert into counterparty_x_uniq_counterparty values (1096,'2015-08-27','2015-11-06',1040,'MDMP');
insert into counterparty_x_uniq_counterparty values (1096,'2015-11-06','2999-12-31',1034,'MDMP');
insert into counterparty_x_uniq_counterparty values (1100,'2020-06-30','2020-09-09',1005,'MDMP');
insert into counterparty_x_uniq_counterparty values (1100,'2020-09-09','2999-12-31',1006,'MDMP');
insert into counterparty_x_uniq_counterparty values (1135,'2017-11-30','2018-03-02',1026,'MDMP');
insert into counterparty_x_uniq_counterparty values (1136,'2018-03-02','2999-12-31',1040,'MDMP');
insert into counterparty_x_uniq_counterparty values (1013,'2012-12-22','2013-01-13',1063,'MDMP');
insert into counterparty_x_uniq_counterparty values (1013,'2013-01-13','2999-12-31',1053,'MDMP');
insert into counterparty_x_uniq_counterparty values (1097,'2020-09-26','2021-08-07',1051,'MDMP');
insert into counterparty_x_uniq_counterparty values (1097,'2021-08-07','2999-12-31',1082,'MDMP');
insert into counterparty_x_uniq_counterparty values (1039,'2016-09-19','2017-06-04',1055,'MDMP');
insert into counterparty_x_uniq_counterparty values (1039,'2017-06-04','2999-12-31',1018,'MDMP');
insert into counterparty_x_uniq_counterparty values (1137,'2018-06-12','2019-04-19',1076,'MDMP');
insert into counterparty_x_uniq_counterparty values (1138,'2019-04-19','2999-12-31',1044,'MDMP');
insert into counterparty_x_uniq_counterparty values (1139,'2020-05-26','2020-10-31',1063,'MDMP');
insert into counterparty_x_uniq_counterparty values (1140,'2020-10-31','2999-12-31',1081,'MDMP');
insert into counterparty_x_uniq_counterparty values (1141,'2014-10-26','2015-02-25',1060,'MDMP');
insert into counterparty_x_uniq_counterparty values (1142,'2015-02-25','2999-12-31',1067,'MDMP');
insert into counterparty_x_uniq_counterparty values (1098,'2015-11-25','2016-10-19',1026,'MDMP');
insert into counterparty_x_uniq_counterparty values (1098,'2016-10-19','2999-12-31',1037,'MDMP');
insert into counterparty_x_uniq_counterparty values (1017,'2011-05-18','2011-09-27',1050,'MDMP');
insert into counterparty_x_uniq_counterparty values (1017,'2011-09-27','2999-12-31',1024,'MDMP');
insert into counterparty_x_uniq_counterparty values (1099,'2020-07-09','2020-11-06',1054,'MDMP');
insert into counterparty_x_uniq_counterparty values (1099,'2020-11-06','2999-12-31',1038,'MDMP');
insert into counterparty_x_uniq_counterparty values (1143,'2019-05-09','2019-06-22',1080,'MDMP');
insert into counterparty_x_uniq_counterparty values (1144,'2019-06-22','2999-12-31',1059,'MDMP');
insert into counterparty_x_uniq_counterparty values (1145,'2019-04-27','2020-01-10',1069,'MDMP');
insert into counterparty_x_uniq_counterparty values (1146,'2020-01-10','2999-12-31',1067,'MDMP');
insert into counterparty_x_uniq_counterparty values (1019,'2014-04-12','2014-12-25',1018,'MDMP');
insert into counterparty_x_uniq_counterparty values (1019,'2014-12-25','2999-12-31',1056,'MDMP');
insert into counterparty_x_uniq_counterparty values (1156,'2014-12-15','2014-12-27',1036,'MDMP');
insert into counterparty_x_uniq_counterparty values (1157,'2014-12-27','2999-12-31',1031,'MDMP');
insert into counterparty_x_uniq_counterparty values (1158,'2014-12-14','2015-04-02',1009,'MDMP');
insert into counterparty_x_uniq_counterparty values (1159,'2015-04-02','2015-07-18',1072,'MDMP');
insert into counterparty_x_uniq_counterparty values (1160,'2015-07-18','2999-12-31',1008,'MDMP');
insert into counterparty_x_uniq_counterparty values (1161,'2017-12-30','2999-12-31',1005,'MDMP');
insert into counterparty_x_uniq_counterparty values (1162,'2019-01-27','2999-12-31',1051,'MDMP');
insert into counterparty_x_uniq_counterparty values (1163,'2015-03-10','2999-12-31',1028,'MDMP');
insert into counterparty_x_uniq_counterparty values (1164,'2013-03-09','2999-12-31',1022,'MDMP');
insert into counterparty_x_uniq_counterparty values (1165,'2020-10-08','2999-12-31',1015,'MDMP');
insert into counterparty_x_uniq_counterparty values (1166,'2016-10-20','2999-12-31',1022,'MDMP');
insert into counterparty_x_uniq_counterparty values (1167,'2020-08-26','2999-12-31',1072,'MDMP');
insert into counterparty_x_uniq_counterparty values (1168,'2017-08-03','2999-12-31',1061,'MDMP');
insert into counterparty_x_uniq_counterparty values (1169,'2018-06-02','2999-12-31',1056,'MDMP');
insert into counterparty_x_uniq_counterparty values (1170,'2015-05-29','2999-12-31',1091,'MDMP');
insert into counterparty_x_uniq_counterparty values (1047,'2013-08-26','2999-12-31',1080,'MDMP');
insert into counterparty_x_uniq_counterparty values (1147,'2015-09-06','2999-12-31',1024,'MDMP');
insert into counterparty_x_uniq_counterparty values (1148,'2011-07-24','2999-12-31',1070,'MDMP');
insert into counterparty_x_uniq_counterparty values (1149,'2018-08-09','2999-12-31',1046,'MDMP');
insert into counterparty_x_uniq_counterparty values (1150,'2012-05-14','2999-12-31',1016,'MDMP');
insert into counterparty_x_uniq_counterparty values (1073,'2015-10-18','2999-12-31',1029,'MDMP');
insert into counterparty_x_uniq_counterparty values (1101,'2012-06-23','2999-12-31',1027,'MDMP');
insert into counterparty_x_uniq_counterparty values (1102,'2011-06-24','2999-12-31',1074,'MDMP');
insert into counterparty_x_uniq_counterparty values (1103,'2018-05-09','2999-12-31',1008,'MDMP');
insert into counterparty_x_uniq_counterparty values (1104,'2019-07-18','2999-12-31',1059,'MDMP');
insert into counterparty_x_uniq_counterparty values (1105,'2013-01-06','2999-12-31',1036,'MDMP');
insert into counterparty_x_uniq_counterparty values (1106,'2017-02-03','2999-12-31',1010,'MDMP');
insert into counterparty_x_uniq_counterparty values (1107,'2015-12-17','2999-12-31',1027,'MDMP');
insert into counterparty_x_uniq_counterparty values (1108,'2015-01-22','2999-12-31',1008,'MDMP');
insert into counterparty_x_uniq_counterparty values (1109,'2012-03-06','2999-12-31',1003,'MDMP');
insert into counterparty_x_uniq_counterparty values (1110,'2013-04-28','2999-12-31',1077,'MDMP');
insert into counterparty_x_uniq_counterparty values (1111,'2017-07-29','2999-12-31',1031,'MDMP');
insert into counterparty_x_uniq_counterparty values (1083,'2019-11-12','2999-12-31',1012,'MDMP');
insert into counterparty_x_uniq_counterparty values (1112,'2015-07-01','2999-12-31',1043,'MDMP');
insert into counterparty_x_uniq_counterparty values (1113,'2016-12-19','2999-12-31',1004,'MDMP');
insert into counterparty_x_uniq_counterparty values (1114,'2016-05-14','2999-12-31',1030,'MDMP');
insert into counterparty_x_uniq_counterparty values (1115,'2014-11-23','2999-12-31',1070,'MDMP');
insert into counterparty_x_uniq_counterparty values (1116,'2015-05-21','2999-12-31',1061,'MDMP');
insert into counterparty_x_uniq_counterparty values (1117,'2019-05-19','2999-12-31',1052,'MDMP');
insert into counterparty_x_uniq_counterparty values (1118,'2017-05-09','2999-12-31',1004,'MDMP');
insert into counterparty_x_uniq_counterparty values (1119,'2020-10-09','2999-12-31',1068,'MDMP');
insert into counterparty_x_uniq_counterparty values (1120,'2015-05-08','2999-12-31',1022,'MDMP');
insert into counterparty_x_uniq_counterparty values (1049,'2013-01-19','2999-12-31',1055,'MDMP');
insert into counterparty_x_uniq_counterparty values (1151,'2018-06-02','2999-12-31',1012,'MDMP');
insert into counterparty_x_uniq_counterparty values (1089,'2011-10-18','2999-12-31',1065,'MDMP');
insert into counterparty_x_uniq_counterparty values (1152,'2011-04-05','2999-12-31',1080,'MDMP');
insert into counterparty_x_uniq_counterparty values (1153,'2013-08-04','2999-12-31',1031,'MDMP');
insert into counterparty_x_uniq_counterparty values (1154,'2015-03-17','2999-12-31',1054,'MDMP');
insert into counterparty_x_uniq_counterparty values (1155,'2020-08-18','2999-12-31',1034,'MDMP');
insert into counterparty_x_uniq_counterparty values (1085,'2019-09-27','2999-12-31',1029,'MDMP');
insert into counterparty_x_uniq_counterparty values (1172,'2015-04-27','2999-12-31',1024,'MDMP');
insert into counterparty_x_uniq_counterparty values (1173,'2017-09-01','2999-12-31',1018,'MDMP');
insert into counterparty_x_uniq_counterparty values (1180,'2019-05-24','2999-12-31',1028,'MDMP');
insert into counterparty_x_uniq_counterparty values (1088,'2017-02-20','2999-12-31',1061,'MDMP');
insert into counterparty_x_uniq_counterparty values (1181,'2011-05-25','2999-12-31',1026,'MDMP');
insert into counterparty_x_uniq_counterparty values (1182,'2011-07-17','2999-12-31',1002,'MDMP');
insert into counterparty_x_uniq_counterparty values (1183,'2018-03-27','2999-12-31',1025,'MDMP');
insert into counterparty_x_uniq_counterparty values (1185,'2012-06-01','2999-12-31',1080,'MDMP');
insert into counterparty_x_uniq_counterparty values (1186,'2011-07-02','2999-12-31',1040,'MDMP');
insert into counterparty_x_uniq_counterparty values (1064,'2014-12-04','2999-12-31',1077,'MDMP');
insert into counterparty_x_uniq_counterparty values (1187,'2013-05-31','2999-12-31',1026,'MDMP');
insert into counterparty_x_uniq_counterparty values (1188,'2014-01-05','2999-12-31',1018,'MDMP');
insert into counterparty_x_uniq_counterparty values (1199,'2011-01-16','2999-12-31',1059,'MDMP');
insert into counterparty_x_uniq_counterparty values (1200,'2018-11-23','2999-12-31',1029,'MDMP');
insert into counterparty_x_uniq_counterparty values (1095,'2020-08-21','2999-12-31',1016,'MDMP');
insert into counterparty_x_uniq_counterparty values (1121,'2013-09-05','2999-12-31',1079,'MDMP');
insert into counterparty_x_uniq_counterparty values (1122,'2015-10-13','2999-12-31',1046,'MDMP');
insert into counterparty_x_uniq_counterparty values (1123,'2020-11-30','2999-12-31',1054,'MDMP');
insert into counterparty_x_uniq_counterparty values (1124,'2018-01-29','2999-12-31',1004,'MDMP');
insert into counterparty_x_uniq_counterparty values (1125,'2019-12-26','2999-12-31',1009,'MDMP');
insert into counterparty_x_uniq_counterparty values (1126,'2015-01-26','2999-12-31',1063,'MDMP');
insert into counterparty_x_uniq_counterparty values (1127,'2011-10-25','2999-12-31',1075,'MDMP');
insert into counterparty_x_uniq_counterparty values (1128,'2015-08-02','2999-12-31',1061,'MDMP');
insert into counterparty_x_uniq_counterparty values (1129,'2020-06-15','2999-12-31',1052,'MDMP');
insert into counterparty_x_uniq_counterparty values (1130,'2014-11-21','2999-12-31',1062,'MDMP');
insert into counterparty_x_uniq_counterparty values (1131,'2020-02-04','2999-12-31',1027,'MDMP');
insert into counterparty_x_uniq_counterparty values (1132,'2013-05-30','2999-12-31',1062,'MDMP');
insert into counterparty_x_uniq_counterparty values (1133,'2014-01-05','2999-12-31',1054,'MDMP');
insert into counterparty_x_uniq_counterparty values (1134,'2019-04-20','2999-12-31',1021,'MDMP');
insert into counterparty_x_uniq_counterparty values (1087,'2012-05-23','2999-12-31',1059,'MDMP');
insert into counterparty_x_uniq_counterparty values (1201,'2013-12-23','2999-12-31',1078,'MDMP');
insert into counterparty_x_uniq_counterparty values (1203,'2014-10-11','2999-12-31',1027,'MDMP');
insert into counterparty_x_uniq_counterparty values (1094,'2015-01-08','2999-12-31',1067,'MDMP');


insert into counterparty_contact values (1011,'2014-10-21','2999-12-31','8-985-BBB-AB-BB','MobileWorkNumber','UNDEF',FALSE,'CFTB');
insert into counterparty_contact values (1095,'2011-07-26','2999-12-31','8-495-BBB-BB-AB','HomePhone','GOOD;;',TRUE,'RTLL');
insert into counterparty_contact values (1001,'2018-02-07','2999-12-31','8-985-BAA-AA-AB','MobilePersonalPhone','GOOD',TRUE,'MDMP');
insert into counterparty_contact values (1135,'2018-06-21','2999-12-31','8-985-ABA-AB-AA','MobilePersonalPhone','GOOD',FALSE,'RTLS');
insert into counterparty_contact values (1136,'2016-12-20','2999-12-31','8-985-ABB-AB-AB','MobileWorkNumber','INCORRECT_DATA',TRUE,'WAYN');
insert into counterparty_contact values (1013,'2020-02-26','2999-12-31','8-985-AAB-AB-AA','MobileWorkNumber','GOOD_REPLACED_CODE',TRUE,'WAYN');
insert into counterparty_contact values (1140,'2019-08-25','2999-12-31','8-985-BBB-AB-AB','MobilePersonalPhone','GOOD',FALSE,'RTLS');
insert into counterparty_contact values (1141,'2017-12-26','2999-12-31','8-985-ABB-AA-BB','MobilePersonalPhone','GOOD',TRUE,'WAYN');
insert into counterparty_contact values (1142,'2016-06-26','2999-12-31','8-985-AAA-BA-BB','MobilePersonalPhone','GOOD',TRUE,'WAYN');
insert into counterparty_contact values (1017,'2013-12-31','2014-01-16','8-985-BBA-AA-BB','NotificPhone','GOOD',TRUE,'WAYN');
insert into counterparty_contact values (1017,'2014-01-16','2999-12-31','8-985-BBB-AB-AB','ATMPhone','GARBAGE',TRUE,'RTLL');
insert into counterparty_contact values (1027,'2012-09-02','2013-03-27','8-985-BBB-AB-BB','NotificPhone','GOOD_REPLACED_CODE',FALSE,'MDMP');
insert into counterparty_contact values (1027,'2013-03-27','2999-12-31','8-985-BBB-AB-BA','MobilePersonalPhone','GOOD',TRUE,'MDMP');
insert into counterparty_contact values (1098,'2012-10-17','2012-12-08','8-985-BAB-AA-BB','MobileWorkNumber','GOOD',TRUE,'RTLL');
insert into counterparty_contact values (1098,'2012-12-08','2999-12-31','8-985-BAA-AA-AA','MobileWorkNumber','GOOD',TRUE,'RTLL');
insert into counterparty_contact values (1003,'2016-05-27','2017-04-14','8-985-ABB-BA-BB','NotificPhone','UNDEF',TRUE,'MDMP');
insert into counterparty_contact values (1003,'2017-04-14','2999-12-31','8-985-BBA-BA-BA','NotificPhone','GOOD;;',FALSE,'MDMP');
insert into counterparty_contact values (1143,'2018-09-09','2018-11-03','8-985-ABA-BA-BA','MobilePersonalPhone','GOOD',TRUE,'RTLL');
insert into counterparty_contact values (1143,'2018-11-03','2999-12-31','8-985-ABA-AA-BA','NotificPhone','GOOD_REPLACED_CODE',TRUE,'WAYN');
insert into counterparty_contact values (1144,'2014-05-16','2015-03-28','8-985-BBA-AB-AA','NotificPhone','GOOD_REPLACED_CODE',TRUE,'WAYN');
insert into counterparty_contact values (1144,'2015-03-28','2999-12-31','8-985-BAB-BB-BA','NotificPhone','GOOD',FALSE,'CFTB');
insert into counterparty_contact values (1145,'2017-04-30','2017-06-03','8-985-AAA-BB-AA','NotificPhone','GOOD;;',TRUE,'WAYN');
insert into counterparty_contact values (1054,'2017-06-03','2999-12-31','8-985-BAA-BA-AB','MobilePersonalPhone','GOOD',TRUE,'MDMP');
insert into counterparty_contact values (1054,'2020-04-30','2020-07-14','8-985-ABB-AB-BA','NotificPhone','GOOD',TRUE,'MDMP');
insert into counterparty_contact values (1146,'2020-07-14','2999-12-31','8-985-BAA-AB-AA','MobileWorkNumber','GOOD;;',FALSE,'RTLS');
insert into counterparty_contact values (1156,'2015-10-04','2016-03-31','8-985-BAB-AA-AB','MobileWorkNumber','GOOD',FALSE,'RTLS');
insert into counterparty_contact values (1156,'2016-03-31','2999-12-31','8-495-AAB-AA-AB','HomePhone','GOOD',TRUE,'WAYN');
insert into counterparty_contact values (1157,'2012-08-26','2012-10-30','8-985-BBA-AA-AA','ATMPhone','GOOD',TRUE,'RTLL');
insert into counterparty_contact values (1157,'2012-10-30','2999-12-31','8-985-AAA-AA-AA','MobilePersonalPhone','GOOD',TRUE,'WAYN');
insert into counterparty_contact values (1158,'2017-02-06','2017-04-30','8-985-ABA-AB-BB','ATMPhone','GOOD',TRUE,'WAYN');
insert into counterparty_contact values (1158,'2017-04-30','2999-12-31','8-985-BAB-BB-AB','NotificPhone','GOOD',TRUE,'RTLL');
insert into counterparty_contact values (1162,'2013-04-27','2014-04-17','8-985-BAA-AB-AB','NotificPhone','GOOD',TRUE,'WAYN');
insert into counterparty_contact values (1162,'2014-04-17','2999-12-31','8-985-BBA-AA-AB','MobilePersonalPhone','GOOD;;',TRUE,'WAYN');
insert into counterparty_contact values (1163,'2011-07-17','2012-04-28','8-985-BBA-AA-BB','NotificPhone','GOOD',FALSE,'CFTB');
insert into counterparty_contact values (1163,'2012-04-28','2999-12-31','8-985-AAB-BB-AB','MobilePersonalPhone','GOOD',TRUE,'WAYN');
insert into counterparty_contact values (1164,'2015-05-14','2999-12-31','8-985-ABA-BA-AB','MobileWorkNumber','GOOD',TRUE,'RTLL');
insert into counterparty_contact values (1005,'2011-06-28','2999-12-31','8-985-AAA-BA-AB','ATMPhone','GOOD',FALSE,'MDMP');
insert into counterparty_contact values (1166,'2015-10-31','2999-12-31','8-985-ABB-BB-AB','ATMPhone','GOOD;;',TRUE,'WAYN');
insert into counterparty_contact values (1028,'2015-07-31','2999-12-31','8-985-BBA-AA-AA','MobilePersonalPhone','GOOD;;',FALSE,'MDMP');
insert into counterparty_contact values (1168,'2017-07-31','2999-12-31','8-985-ABB-BB-AA','MobilePersonalPhone','GOOD',TRUE,'WAYN');
insert into counterparty_contact values (1169,'2011-01-07','2999-12-31','8-985-BBB-AA-AA','MobilePersonalPhone','GOOD',TRUE,'WAYN');
insert into counterparty_contact values (1170,'2013-09-09','2999-12-31','8-985-BBA-BA-BB','MobilePersonalPhone','INCORRECT_DATA',TRUE,'WAYN');
insert into counterparty_contact values (1047,'2016-05-05','2999-12-31','8-985-BAB-AA-AA','MobilePersonalPhone','GOOD',TRUE,'WAYN');
insert into counterparty_contact values (1147,'2011-03-08','2999-12-31','8-985-BBA-BB-BB','MobilePersonalPhone','GOOD',TRUE,'RTLL');
insert into counterparty_contact values (1032,'2015-12-27','2999-12-31','8-985-BBB-AA-AA','MobilePersonalPhone','GOOD',TRUE,'MDMP');
insert into counterparty_contact values (1149,'2014-03-19','2999-12-31','8-985-BBA-BA-AB','MobilePersonalPhone','GOOD',FALSE,'RTLS');
insert into counterparty_contact values (1008,'2011-07-26','2999-12-31','8-985-BBB-BB-AB','MobilePersonalPhone','GOOD',FALSE,'MDMP');
insert into counterparty_contact values (1073,'2019-09-03','2999-12-31','8-495-BBB-AB-BB','HomePhone','GOOD',FALSE,'CFTB');
insert into counterparty_contact values (1101,'2015-08-21','2999-12-31','8-495-BBB-BA-AA','HomePhone','GOOD;;',TRUE,'WAYN');
insert into counterparty_contact values (1050,'2015-07-20','2015-08-05','8-495-ABB-AA-BA','HomePhone','GOOD',TRUE,'MDMP');
insert into counterparty_contact values (1050,'2015-08-05','2999-12-31','8-495-ABA-BB-AB','HomePhone','GOOD',TRUE,'MDMP');
insert into counterparty_contact values (1109,'2016-01-28','2016-11-07','8-495-BBB-BB-BA','HomePhone','GOOD',FALSE,'RTLS');
insert into counterparty_contact values (1109,'2016-11-07','2999-12-31','8-985-BBA-AA-BA','MobilePersonalPhone','GOOD',TRUE,'WAYN');
insert into counterparty_contact values (1032,'2016-02-11','2016-03-20','8-985-BBB-AA-BB','MobileWorkNumber','GOOD',FALSE,'MDMP');
insert into counterparty_contact values (1032,'2016-03-20','2999-12-31','8-985-BBB-BA-BA','MobileWorkNumber','GOOD',TRUE,'MDMP');
insert into counterparty_contact values (1045,'2015-09-13','2016-03-18','8-985-BBB-AA-BA','MobileWorkNumber','GOOD',TRUE,'MDMP');
insert into counterparty_contact values (1045,'2016-03-18','2999-12-31','8-985-ABA-AB-BA','MobilePersonalPhone','GOOD',TRUE,'MDMP');
insert into counterparty_contact values (1112,'2014-05-17','2015-02-05','8-985-BAB-BB-AB','MobilePersonalPhone','GOOD;;',TRUE,'RTLL');
insert into counterparty_contact values (1112,'2015-02-05','2999-12-31','8-985-BAA-AB-BB','ATMPhone','GOOD;;',TRUE,'WAYN');
insert into counterparty_contact values (1113,'2011-03-26','2011-11-28','8-985-BAB-BB-AB','NotificPhone','GOOD;;',TRUE,'WAYN');
insert into counterparty_contact values (1113,'2011-11-28','2999-12-31','8-985-ABB-AA-BA','ATMPhone','GOOD;;',FALSE,'RTLS');
insert into counterparty_contact values (1038,'2019-11-18','2020-11-14','8-985-ABA-BB-BB','MobilePersonalPhone','UNDEF',FALSE,'MDMP');
insert into counterparty_contact values (1038,'2020-11-14','2999-12-31','8-985-BAA-BB-BA','MobilePersonalPhone','GARBAGE',TRUE,'MDMP');
insert into counterparty_contact values (1115,'2018-10-29','2018-12-09','8-985-AAA-AA-BB','NotificPhone','GOOD_REPLACED_CODE',TRUE,'WAYN');
insert into counterparty_contact values (1115,'2018-12-09','2999-12-31','8-985-AAA-BB-AA','ATMPhone','GOOD;;',TRUE,'RTLL');
insert into counterparty_contact values (1116,'2012-11-20','2013-11-06','8-985-BBB-BB-BA','NotificPhone','GOOD_REPLACED_CODE',TRUE,'WAYN');
insert into counterparty_contact values (1116,'2013-11-06','2999-12-31','8-985-AAB-AB-BA','MobilePersonalPhone','GOOD_REPLACED_CODE',TRUE,'WAYN');
insert into counterparty_contact values (1117,'2018-01-04','2018-06-28','8-985-ABB-BB-BB','NotificPhone','GOOD',TRUE,'WAYN');
insert into counterparty_contact values (1117,'2018-06-28','2999-12-31','8-495-ABB-BB-BB','HomePhone','GOOD',FALSE,'CFTB');
insert into counterparty_contact values (1012,'2014-03-15','2014-08-20','8-495-ABA-BA-BB','HomePhone','GOOD',FALSE,'MDMP');
insert into counterparty_contact values (1012,'2014-08-20','2999-12-31','8-495-BBB-AA-AA','HomePhone','GOOD',TRUE,'MDMP');
insert into counterparty_contact values (1119,'2014-04-30','2014-06-11','8-495-ABA-AB-AB','HomePhone','GOOD',TRUE,'WAYN');
insert into counterparty_contact values (1119,'2014-06-11','2999-12-31','8-495-ABA-AB-AB','HomePhone','GOOD_REPLACED_CODE',TRUE,'WAYN');
insert into counterparty_contact values (1120,'2014-11-18','2015-11-17','8-985-AAA-AB-BA','NotificPhone','GOOD_REPLACED_CODE',TRUE,'WAYN');
insert into counterparty_contact values (1120,'2015-11-17','2999-12-31','8-985-BAA-BA-AB','MobileWorkNumber','GOOD_REPLACED_CODE',TRUE,'WAYN');
insert into counterparty_contact values (1049,'2013-09-12','2999-12-31','8-985-BBA-AA-AA','MobileWorkNumber','GOOD_REPLACED_CODE',TRUE,'RTLL');
insert into counterparty_contact values (1151,'2013-01-06','2999-12-31','8-495-ABA-AB-AB','HomePhone','GOOD_REPLACED_CODE',TRUE,'WAYN');
insert into counterparty_contact values (1089,'2017-07-01','2999-12-31','8-985-BAB-BA-BA','MobileWorkNumber','GOOD_REPLACED_CODE',TRUE,'WAYN');
insert into counterparty_contact values (1152,'2019-12-17','2999-12-31','8-985-AAA-AB-BB','MobileWorkNumber','GOOD_REPLACED_CODE',FALSE,'RTLS');
insert into counterparty_contact values (1153,'2020-10-21','2999-12-31','8-985-BAA-AB-BB','MobileWorkNumber','GOOD',TRUE,'WAYN');
insert into counterparty_contact values (1154,'2012-03-16','2999-12-31','8-985-AAB-AA-AA','MobileWorkNumber','GOOD',TRUE,'RTLL');
insert into counterparty_contact values (1155,'2011-11-20','2999-12-31','8-985-BAB-BB-AA','MobileWorkNumber','GOOD',TRUE,'WAYN');
insert into counterparty_contact values (1085,'2020-11-05','2999-12-31','8-985-BAA-AA-AA','MobileWorkNumber','GOOD;;',TRUE,'WAYN');
insert into counterparty_contact values (1172,'2020-08-10','2999-12-31','8-985-AAA-AB-BA','MobileWorkNumber','GOOD;;',TRUE,'RTLL');
insert into counterparty_contact values (1173,'2019-03-04','2999-12-31','8-985-ABA-BA-BA','MobileWorkNumber','GOOD',TRUE,'WAYN');
insert into counterparty_contact values (1080,'2011-04-13','2999-12-31','8-985-ABA-AA-AA','MobileWorkNumber','GOOD',TRUE,'MDMP');
insert into counterparty_contact values (1088,'2018-03-08','2999-12-31','8-985-ABB-BA-AB','MobileWorkNumber','GOOD',TRUE,'WAYN');
insert into counterparty_contact values (1185,'2017-03-09','2999-12-31','8-495-AAB-BA-AA','HomePhone','GOOD',TRUE,'WAYN');
insert into counterparty_contact values (1186,'2018-11-14','2999-12-31','8-495-ABB-AA-AB','HomePhone','GOOD',TRUE,'WAYN');
insert into counterparty_contact values (1064,'2012-12-25','2999-12-31','8-495-AAB-BB-AB','HomePhone','GOOD',FALSE,'CFTB');
insert into counterparty_contact values (1187,'2017-09-10','2999-12-31','8-495-BAB-BA-BB','HomePhone','GOOD;;',TRUE,'WAYN');
insert into counterparty_contact values (1188,'2020-01-04','2999-12-31','8-495-AAA-BA-AA','HomePhone','GOOD',TRUE,'WAYN');
insert into counterparty_contact values (1199,'2019-12-27','2999-12-31','8-495-AAA-BA-BB','HomePhone','GOOD',TRUE,'WAYN');
insert into counterparty_contact values (1123,'2019-05-04','2999-12-31','8-985-BAA-BA-AA','MobileWorkNumber','GOOD',TRUE,'RTLL');
insert into counterparty_contact values (1092,'2012-07-23','2999-12-31','8-985-BBA-BA-BB','MobileWorkNumber','GOOD;;',TRUE,'MDMP');
insert into counterparty_contact values (1087,'2013-12-04','2999-12-31','8-985-ABA-AB-AB','ATMPhone','GOOD;;',TRUE,'WAYN');
insert into counterparty_contact values (1201,'2013-02-11','2999-12-31','8-985-BAA-AA-BB','MobilePersonalPhone','GOOD',FALSE,'RTLS');
insert into counterparty_contact values (1203,'2017-07-01','2999-12-31','8-985-BAA-AA-AA','MobilePersonalPhone','GOOD',TRUE,'WAYN');
insert into counterparty_contact values (1094,'2012-01-05','2999-12-31','8-985-AAB-BB-BB','MobilePersonalPhone','GOOD',TRUE,'WAYN');
insert into counterparty_contact values (1011,'2019-01-13','2999-12-31','8-985-ABB-BA-AA','MobilePersonalPhone','GOOD',TRUE,'RTLL');
insert into counterparty_contact values (1050,'2012-05-25','2999-12-31','8-985-BAA-BB-BB','MobilePersonalPhone','GARBAGE',FALSE,'MDMP');
insert into counterparty_contact values (1099,'2011-12-14','2999-12-31','8-985-AAA-AB-AA','ATMPhone','GOOD;;',TRUE,'RTLL');
insert into counterparty_contact values (1143,'2015-05-18','2999-12-31','8-985-BAA-AA-BA','ATMPhone','INCORRECT_DATA',TRUE,'WAYN');
insert into counterparty_contact values (1144,'2020-12-28','2999-12-31','8-495-AAB-BA-BB','HomePhone','GOOD',TRUE,'RTLL');
insert into counterparty_contact values (1145,'2019-02-22','2999-12-31','8-985-ABA-BB-BB','MobileWorkNumber','GOOD;;',TRUE,'WAYN');
insert into counterparty_contact values (1032,'2013-06-03','2999-12-31','8-985-ABB-AB-AA','MobileWorkNumber','GOOD;;',FALSE,'MDMP');
insert into counterparty_contact values (1156,'2016-12-02','2999-12-31','8-985-ABB-AA-AB','MobileWorkNumber','GOOD;;',TRUE,'RTLL');
insert into counterparty_contact values (1157,'2016-05-15','2999-12-31','8-985-BAB-AA-AA','NotificPhone','GOOD;;',TRUE,'RTLL');
insert into counterparty_contact values (1093,'2011-01-18','2999-12-31','8-985-ABA-AA-BB','MobilePersonalPhone','GOOD;;',TRUE,'MDMP');
insert into counterparty_contact values (1162,'2019-07-28','2999-12-31','8-985-AAA-BA-BB','NotificPhone','GOOD;;',TRUE,'WAYN');
insert into counterparty_contact values (1163,'2016-02-23','2999-12-31','8-985-BAA-BA-AB','NotificPhone','GOOD;;',TRUE,'RTLL');
insert into counterparty_contact values (1164,'2020-02-20','2999-12-31','8-495-ABB-BB-AB','HomePhone','GOOD;;',FALSE,'CFTB');
insert into counterparty_contact values (1111,'2016-06-05','2999-12-31','8-495-BAB-BB-AA','HomePhone','GOOD;;',TRUE,'WAYN');
insert into counterparty_contact values (1112,'2017-12-07','2999-12-31','8-495-ABB-BA-AA','HomePhone','GOOD;;',TRUE,'RTLL');
insert into counterparty_contact values (1117,'2012-11-26','2999-12-31','8-495-BAB-BA-AA','HomePhone','GOOD',TRUE,'RTLL');
insert into counterparty_contact values (1118,'2012-04-02','2999-12-31','8-985-BBB-BA-AA','MobilePersonalPhone','GOOD',TRUE,'WAYN');
insert into counterparty_contact values (1119,'2019-02-16','2999-12-31','8-985-AAB-BA-AB','NotificPhone','GOOD',TRUE,'WAYN');
insert into counterparty_contact values (1120,'2019-01-19','2999-12-31','8-985-ABB-BB-AB','NotificPhone','GOOD',TRUE,'WAYN');
insert into counterparty_contact values (1050,'2016-06-26','2999-12-31','8-985-ABA-AB-AB','ATMPhone','INCORRECT_DATA',TRUE,'MDMP');
insert into counterparty_contact values (1151,'2018-02-02','2999-12-31','8-985-ABB-AA-BB','MobilePersonalPhone','GOOD',TRUE,'WAYN');
insert into counterparty_contact values (1089,'2015-09-07','2999-12-31','8-985-BBA-AB-AA','MobilePersonalPhone','GOOD;;',TRUE,'RTLL');
insert into counterparty_contact values (1152,'2018-06-19','2999-12-31','8-985-BAB-BA-BA','MobilePersonalPhone','GOOD',TRUE,'WAYN');
insert into counterparty_contact values (1153,'2013-03-14','2999-12-31','8-985-BBA-AB-AB','MobilePersonalPhone','GOOD;;',TRUE,'WAYN');
insert into counterparty_contact values (1154,'2013-04-14','2999-12-31','8-495-BAB-AA-AB','HomePhone','GOOD',TRUE,'WAYN');
insert into counterparty_contact values (1155,'2013-05-12','2999-12-31','8-985-BBB-BB-BB','MobilePersonalPhone','GOOD',TRUE,'WAYN');
insert into counterparty_contact values (1186,'2019-03-31','2999-12-31','8-985-BAA-AB-BB','MobilePersonalPhone','UNDEF',TRUE,'WAYN');
insert into counterparty_contact values (1032,'2017-06-16','2999-12-31','8-985-AAA-AA-BA','MobilePersonalPhone','GOOD',TRUE,'MDMP');
insert into counterparty_contact values (1187,'2016-09-13','2999-12-31','8-985-BAA-BA-AA','MobilePersonalPhone','GOOD',TRUE,'RTLL');
insert into counterparty_contact values (1188,'2014-06-10','2999-12-31','8-985-ABA-BA-BB','ATMPhone','GOOD;;',TRUE,'WAYN');
insert into counterparty_contact values (1199,'2013-01-27','2999-12-31','8-495-BBB-AA-AB','HomePhone','GOOD',TRUE,'WAYN');
insert into counterparty_contact values (1123,'2017-04-08','2999-12-31','8-495-BAB-AB-BB','HomePhone','GOOD',TRUE,'WAYN');
insert into counterparty_contact values (1134,'2016-11-13','2999-12-31','8-495-BAA-BA-BA','HomePhone','GOOD;;',TRUE,'RTLL');
insert into counterparty_contact values (1038,'2019-06-14','2999-12-31','8-495-ABA-BA-AB','HomePhone','GOOD;;',FALSE,'MDMP');
insert into counterparty_contact values (1011,'2016-08-12','2999-12-31','8-495-BBA-AA-BB','HomePhone','GOOD;;',TRUE,'WAYN');
insert into counterparty_contact values (1147,'2016-03-19','2999-12-31','8-495-ABB-AA-AA','HomePhone','GOOD',TRUE,'WAYN');
insert into counterparty_contact values (1148,'2015-12-08','2999-12-31','8-985-BAB-BA-AB','NotificPhone','GOOD;;',TRUE,'WAYN');
insert into counterparty_contact values (1149,'2019-08-23','2999-12-31','8-985-BAB-AB-AB','MobileWorkNumber','GOOD',TRUE,'WAYN');
insert into counterparty_contact values (1150,'2011-11-15','2999-12-31','8-985-BAB-AA-AA','MobilePersonalPhone','GOOD',TRUE,'WAYN');
insert into counterparty_contact values (1073,'2020-06-24','2999-12-31','8-495-BAB-BA-AA','HomePhone','GOOD',TRUE,'RTLL');
insert into counterparty_contact values (1101,'2012-05-18','2999-12-31','8-985-AAB-BB-AA','ATMPhone','GOOD;;',TRUE,'WAYN');
insert into counterparty_contact values (1116,'2016-11-18','2999-12-31','8-985-AAA-AA-AA','NotificPhone','GOOD_REPLACED_CODE',TRUE,'WAYN');
insert into counterparty_contact values (1134,'2013-06-25','2999-12-31','8-985-BBA-BB-BA','ATMPhone','GOOD_REPLACED_CODE',TRUE,'WAYN');
insert into counterparty_contact values (1087,'2011-05-15','2999-12-31','8-985-BAA-AB-AB','NotificPhone','GOOD',TRUE,'RTLL');
insert into counterparty_contact values (1201,'2018-03-05','2999-12-31','8-985-BBB-BA-AA','ATMPhone','GOOD',TRUE,'WAYN');
insert into counterparty_contact values (1203,'2017-05-19','2999-12-31','8-495-BAB-AA-AA','HomePhone','GOOD;;',TRUE,'WAYN');
insert into counterparty_contact values (1094,'2013-07-13','2999-12-31','8-495-BBB-AB-BB','HomePhone','GOOD',TRUE,'RTLL');
insert into counterparty_contact values (1050,'2020-08-29','2999-12-31','8-495-BBA-AB-AA','HomePhone','GOOD',FALSE,'MDMP');
insert into counterparty_contact values (1095,'2018-11-05','2999-12-31','8-495-ABA-BB-AB','HomePhone','GOOD',TRUE,'WAYN');
insert into counterparty_contact values (1162,'2014-03-15','2999-12-31','8-495-BAB-AB-AB','HomePhone','GOOD',TRUE,'WAYN');
insert into counterparty_contact values (1093,'2014-06-08','2999-12-31','8-985-BBA-BA-BA','ATMPhone','GOOD',TRUE,'MDMP');
insert into counterparty_contact values (1164,'2019-01-04','2999-12-31','8-985-ABB-AB-AA','MobilePersonalPhone','GOOD_REPLACED_CODE',TRUE,'WAYN');
insert into counterparty_contact values (1165,'2014-11-17','2999-12-31','8-985-BBA-AA-BA','MobilePersonalPhone','GOOD;;',TRUE,'RTLL');
insert into counterparty_contact values (1166,'2012-03-29','2999-12-31','8-985-AAB-BB-BB','MobilePersonalPhone','GOOD_REPLACED_CODE',TRUE,'WAYN');
insert into counterparty_contact values (1167,'2013-07-04','2999-12-31','8-985-ABB-BA-AA','MobilePersonalPhone','GOOD_REPLACED_CODE',TRUE,'WAYN');
insert into counterparty_contact values (1168,'2014-09-15','2999-12-31','8-985-BBA-BB-BA','MobilePersonalPhone','GOOD',FALSE,'CFTB');
insert into counterparty_contact values (1116,'2011-06-10','2999-12-31','8-985-AAA-BB-BA','MobilePersonalPhone','GOOD',TRUE,'RTLL');
insert into counterparty_contact values (1117,'2018-09-12','2999-12-31','8-985-ABA-AA-BB','MobilePersonalPhone','GOOD_REPLACED_CODE',TRUE,'WAYN');
insert into counterparty_contact values (1118,'2018-08-19','2999-12-31','8-495-ABB-BA-AA','HomePhone','GOOD;;',TRUE,'WAYN');
insert into counterparty_contact values (1119,'2013-11-05','2999-12-31','8-985-BAA-AA-BA','MobileWorkNumber','GOOD;;',TRUE,'WAYN');
insert into counterparty_contact values (1032,'2018-12-25','2999-12-31','8-985-AAA-BA-BB','MobilePersonalPhone','GOOD_REPLACED_CODE',FALSE,'MDMP');
insert into counterparty_contact values (1049,'2011-05-06','2999-12-31','8-985-ABA-BA-BA','ATMPhone','GOOD_REPLACED_CODE',TRUE,'WAYN');
insert into counterparty_contact values (1151,'2013-01-07','2999-12-31','8-985-BAB-AB-BA','MobilePersonalPhone','GOOD_REPLACED_CODE',TRUE,'WAYN');
insert into counterparty_contact values (1064,'2020-11-10','2999-12-31','8-985-ABB-AA-AB','NotificPhone','GOOD_REPLACED_CODE',TRUE,'WAYN');
insert into counterparty_contact values (1187,'2011-05-02','2999-12-31','8-985-BBA-AA-AB','NotificPhone','GOOD_REPLACED_CODE',TRUE,'WAYN');
insert into counterparty_contact values (1038,'2016-09-19','2999-12-31','8-985-BBB-BB-AB','MobilePersonalPhone','GOOD_REPLACED_CODE',TRUE,'MDMP');
insert into counterparty_contact values (1011,'2018-05-30','2999-12-31','8-985-ABA-AA-BA','NotificPhone','GOOD',TRUE,'RTLL');
insert into counterparty_contact values (1095,'2012-03-17','2999-12-31','8-495-BBB-BB-AA','HomePhone','GOOD',TRUE,'WAYN');
insert into counterparty_contact values (1100,'2019-02-17','2999-12-31','8-495-BBB-AB-AB','HomePhone','GOOD;;',TRUE,'RTLL');
insert into counterparty_contact values (1135,'2020-08-15','2999-12-31','8-495-ABA-AA-BB','HomePhone','GOOD',TRUE,'WAYN');
insert into counterparty_contact values (1136,'2020-06-26','2999-12-31','8-495-BBB-AB-AB','HomePhone','GOOD;;',TRUE,'RTLL');
insert into counterparty_contact values (1019,'2014-12-19','2999-12-31','8-495-BAA-AB-BA','HomePhone','GOOD',TRUE,'RTLL');
insert into counterparty_contact values (1098,'2016-05-02','2999-12-31','8-985-BAB-AB-BA','NotificPhone','GOOD',TRUE,'WAYN');
insert into counterparty_contact values (1099,'2013-05-22','2999-12-31','8-985-AAB-AB-BA','NotificPhone','GOOD',TRUE,'WAYN');
insert into counterparty_contact values (1143,'2017-01-10','2999-12-31','8-985-BBA-AB-BB','MobilePersonalPhone','GOOD',TRUE,'RTLL');
insert into counterparty_contact values (1144,'2016-07-22','2999-12-31','8-985-BBB-AB-AB','ATMPhone','GOOD;;',TRUE,'WAYN');
insert into counterparty_contact values (1145,'2014-02-04','2999-12-31','8-985-BBB-AA-AA','NotificPhone','GOOD',TRUE,'WAYN');
insert into counterparty_contact values (1050,'2013-05-15','2999-12-31','8-495-BAA-AA-AA','HomePhone','GOOD',TRUE,'MDMP');
insert into counterparty_contact values (1148,'2019-06-13','2999-12-31','8-985-AAA-BA-AB','NotificPhone','GOOD',TRUE,'RTLL');
insert into counterparty_contact values (1149,'2011-02-11','2999-12-31','8-985-BAA-AB-AA','MobilePersonalPhone','UNDEF',TRUE,'WAYN');
insert into counterparty_contact values (1032,'2011-12-04','2999-12-31','8-985-ABA-AB-BB','MobileWorkNumber','GOOD;;',FALSE,'MDMP');
insert into counterparty_contact values (1073,'2013-01-25','2999-12-31','8-985-BBB-AA-BA','ATMPhone','INCORRECT_DATA',TRUE,'RTLL');
insert into counterparty_contact values (1101,'2020-11-24','2999-12-31','8-495-ABA-AA-AA','HomePhone','GOOD',TRUE,'WAYN');
insert into counterparty_contact values (1108,'2013-04-02','2999-12-31','8-495-BAA-AB-AA','HomePhone','GOOD',TRUE,'WAYN');
insert into counterparty_contact values (1093,'2018-06-08','2999-12-31','8-495-BBA-AB-AA','HomePhone','GOOD;;',TRUE,'MDMP');
insert into counterparty_contact values (1110,'2013-07-06','2999-12-31','8-495-AAB-BB-BB','HomePhone','GOOD',TRUE,'WAYN');
insert into counterparty_contact values (1203,'2019-07-27','2999-12-31','8-495-BAB-AB-BB','HomePhone','GOOD',TRUE,'RTLL');
insert into counterparty_contact values (1094,'2020-02-06','2999-12-31','8-495-AAB-AA-AB','HomePhone','GARBAGE',TRUE,'WAYN');



