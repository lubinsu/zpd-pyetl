/*
 Navicat Premium Data Transfer

 Source Server         : dev_192.168.41.135_mysql_5.7_3306
 Source Server Type    : MySQL
 Source Server Version : 50729
 Source Host           : 192.168.41.135:3306
 Source Schema         : etl_interface

 Target Server Type    : MySQL
 Target Server Version : 50729
 File Encoding         : 65001

 Date: 31/08/2021 11:05:18
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;
set global log_bin_trust_function_creators = 1;

-- ----------------------------
-- Table structure for py_connections
-- ----------------------------
CREATE TABLE `py_connections` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  `name` varchar(255) NOT NULL COMMENT '自定义数据库连接名称',
  `db` varchar(255) NOT NULL COMMENT '数据库名称',
  `db_type_id` int(11) NOT NULL COMMENT '数据库类型：来源：py_dbtype',
  `host` varchar(255) NOT NULL COMMENT '数据库IP',
  `port` varchar(255) NOT NULL COMMENT '数据库端口',
  `user` varchar(255) NOT NULL COMMENT '用户名',
  `password` varchar(255) NOT NULL COMMENT '密码',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `ind_connn_name` (`name`) USING BTREE,
  KEY `dbtype_id` (`db_type_id`) USING BTREE,
  CONSTRAINT `py_connections_ibfk_1` FOREIGN KEY (`db_type_id`) REFERENCES `py_dbtype` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;
-- ----------------------------
-- Records of py_connections
-- ----------------------------
INSERT INTO `py_connections` VALUES (1, 'ydhl', 'ydhl', 1, '192.168.41.135', '3306', 'root', 'TVRJek5EVTI=');
INSERT INTO `py_connections` VALUES (2, 'his', 'orcl', 2, 'lubinsu', '1521', 'system', 'TVRJek5EVTI=');
INSERT INTO `py_connections` VALUES (3, 'his_pg', 'his_test', 4, '192.168.41.135', '5432', 'postgres', 'TVRJek5EVTI=');

-- ----------------------------
-- Table structure for py_dbtype
-- ----------------------------
CREATE TABLE `py_dbtype`  (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  `dbType` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '当前支持的数据库类型',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 5 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = DYNAMIC;

-- ----------------------------
-- Records of py_dbtype
-- ----------------------------
INSERT INTO `py_dbtype` VALUES (1, 'mysql');
INSERT INTO `py_dbtype` VALUES (2, 'oracle');
INSERT INTO `py_dbtype` VALUES (3, 'sqlserver');
INSERT INTO `py_dbtype` VALUES (4, 'postgre');

-- ----------------------------
-- Table structure for py_etl_run_log
-- ----------------------------
CREATE TABLE `py_etl_run_log`  (
  `id` int(32) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `pid` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '服务器进程号',
  `job_chain` varchar(3000) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '任务链',
  `current_job` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '当前执行的job名称',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '日志记录时间',
  `status` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '日志状态：DEBUG/INFO/WARNING/ERROR，如果ERROR则表示报错',
  `message` varchar(3000) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '日志信息',
  `line_no` int(32) NULL DEFAULT NULL COMMENT '程序代码行',
  PRIMARY KEY (`id`, `create_time`) USING BTREE,
  INDEX `in_pid`(`pid`) USING BTREE,
  INDEX `in_create_time`(`create_time`) USING BTREE,
  INDEX `in_current_job`(`current_job`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = DYNAMIC PARTITION BY RANGE (DAYOFMONTH(create_time))
PARTITIONS 33
(PARTITION `p0` VALUES LESS THAN (1) ENGINE = InnoDB MAX_ROWS = 0 MIN_ROWS = 0 ,
PARTITION `p1` VALUES LESS THAN (2) ENGINE = InnoDB MAX_ROWS = 0 MIN_ROWS = 0 ,
PARTITION `p2` VALUES LESS THAN (3) ENGINE = InnoDB MAX_ROWS = 0 MIN_ROWS = 0 ,
PARTITION `p3` VALUES LESS THAN (4) ENGINE = InnoDB MAX_ROWS = 0 MIN_ROWS = 0 ,
PARTITION `p4` VALUES LESS THAN (5) ENGINE = InnoDB MAX_ROWS = 0 MIN_ROWS = 0 ,
PARTITION `p5` VALUES LESS THAN (6) ENGINE = InnoDB MAX_ROWS = 0 MIN_ROWS = 0 ,
PARTITION `p6` VALUES LESS THAN (7) ENGINE = InnoDB MAX_ROWS = 0 MIN_ROWS = 0 ,
PARTITION `p7` VALUES LESS THAN (8) ENGINE = InnoDB MAX_ROWS = 0 MIN_ROWS = 0 ,
PARTITION `p8` VALUES LESS THAN (9) ENGINE = InnoDB MAX_ROWS = 0 MIN_ROWS = 0 ,
PARTITION `p9` VALUES LESS THAN (10) ENGINE = InnoDB MAX_ROWS = 0 MIN_ROWS = 0 ,
PARTITION `p10` VALUES LESS THAN (11) ENGINE = InnoDB MAX_ROWS = 0 MIN_ROWS = 0 ,
PARTITION `p11` VALUES LESS THAN (12) ENGINE = InnoDB MAX_ROWS = 0 MIN_ROWS = 0 ,
PARTITION `p12` VALUES LESS THAN (13) ENGINE = InnoDB MAX_ROWS = 0 MIN_ROWS = 0 ,
PARTITION `p13` VALUES LESS THAN (14) ENGINE = InnoDB MAX_ROWS = 0 MIN_ROWS = 0 ,
PARTITION `p14` VALUES LESS THAN (15) ENGINE = InnoDB MAX_ROWS = 0 MIN_ROWS = 0 ,
PARTITION `p15` VALUES LESS THAN (16) ENGINE = InnoDB MAX_ROWS = 0 MIN_ROWS = 0 ,
PARTITION `p16` VALUES LESS THAN (17) ENGINE = InnoDB MAX_ROWS = 0 MIN_ROWS = 0 ,
PARTITION `p17` VALUES LESS THAN (18) ENGINE = InnoDB MAX_ROWS = 0 MIN_ROWS = 0 ,
PARTITION `p18` VALUES LESS THAN (19) ENGINE = InnoDB MAX_ROWS = 0 MIN_ROWS = 0 ,
PARTITION `p19` VALUES LESS THAN (20) ENGINE = InnoDB MAX_ROWS = 0 MIN_ROWS = 0 ,
PARTITION `p20` VALUES LESS THAN (21) ENGINE = InnoDB MAX_ROWS = 0 MIN_ROWS = 0 ,
PARTITION `p21` VALUES LESS THAN (22) ENGINE = InnoDB MAX_ROWS = 0 MIN_ROWS = 0 ,
PARTITION `p22` VALUES LESS THAN (23) ENGINE = InnoDB MAX_ROWS = 0 MIN_ROWS = 0 ,
PARTITION `p23` VALUES LESS THAN (24) ENGINE = InnoDB MAX_ROWS = 0 MIN_ROWS = 0 ,
PARTITION `p24` VALUES LESS THAN (25) ENGINE = InnoDB MAX_ROWS = 0 MIN_ROWS = 0 ,
PARTITION `p25` VALUES LESS THAN (26) ENGINE = InnoDB MAX_ROWS = 0 MIN_ROWS = 0 ,
PARTITION `p26` VALUES LESS THAN (27) ENGINE = InnoDB MAX_ROWS = 0 MIN_ROWS = 0 ,
PARTITION `p27` VALUES LESS THAN (28) ENGINE = InnoDB MAX_ROWS = 0 MIN_ROWS = 0 ,
PARTITION `p28` VALUES LESS THAN (29) ENGINE = InnoDB MAX_ROWS = 0 MIN_ROWS = 0 ,
PARTITION `p29` VALUES LESS THAN (30) ENGINE = InnoDB MAX_ROWS = 0 MIN_ROWS = 0 ,
PARTITION `p30` VALUES LESS THAN (31) ENGINE = InnoDB MAX_ROWS = 0 MIN_ROWS = 0 ,
PARTITION `p31` VALUES LESS THAN (32) ENGINE = InnoDB MAX_ROWS = 0 MIN_ROWS = 0 ,
PARTITION `p_max` VALUES LESS THAN (MAXVALUE) ENGINE = InnoDB MAX_ROWS = 0 MIN_ROWS = 0 )
;

-- ----------------------------
-- Table structure for py_header_cfg
-- ----------------------------
CREATE TABLE `py_header_cfg`  (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `trans_id` int(11) NOT NULL COMMENT '对应的转换',
  `header_key` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT 'key',
  `header_value` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT 'value',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `fk_trans_id`(`trans_id`) USING BTREE,
  CONSTRAINT `fk_trans_id` FOREIGN KEY (`trans_id`) REFERENCES `py_transforms` (`id`) ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE = InnoDB AUTO_INCREMENT = 2 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = 'header配置表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of py_header_cfg
-- ----------------------------

-- ----------------------------
-- Table structure for py_jobs
-- ----------------------------
CREATE TABLE `py_jobs`  (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  `name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '自定义job名称',
  `job_type_id` int(11) NOT NULL COMMENT 'job类型，关联：py_jobtype',
  `source_conn_id` int(11) NOT NULL COMMENT '来源数据库',
  `source_sql` varchar(5000) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '取数SQL，如果是存储过程则写调用语句call pro( )，Oracle则填写调用语句块，包含BEGIN...END',
  `state` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'Y' COMMENT '状态，默认Y，N则为不在用',
  `comments` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '备注，job说明',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `job_name`(`name`) USING BTREE,
  INDEX `job_type_id`(`job_type_id`) USING BTREE,
  INDEX `source_connection_id`(`source_conn_id`) USING BTREE,
  CONSTRAINT `py_jobs_ibfk_1` FOREIGN KEY (`job_type_id`) REFERENCES `py_jobtype` (`id`) ON DELETE RESTRICT ON UPDATE CASCADE,
  CONSTRAINT `py_jobs_ibfk_2` FOREIGN KEY (`source_conn_id`) REFERENCES `py_connections` (`id`) ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE = InnoDB AUTO_INCREMENT = 14 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = 'JOB配置表' ROW_FORMAT = DYNAMIC;


-- ----------------------------
-- Table structure for py_jobtype
-- ----------------------------
CREATE TABLE `py_jobtype`  (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  `jobType` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '目前支持的job类型',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 5 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = DYNAMIC;

-- ----------------------------
-- Records of py_jobtype
-- ----------------------------
INSERT INTO `py_jobtype` VALUES (1, 'syn');
INSERT INTO `py_jobtype` VALUES (2, 'procedure');
INSERT INTO `py_jobtype` VALUES (3, 'stream');
INSERT INTO `py_jobtype` VALUES (4, 'syn_dynamic');

-- ----------------------------
-- Table structure for py_procedure_params
-- ----------------------------
CREATE TABLE `py_procedure_params`  (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  `name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '存储过程参数名称',
  `in_out_type` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '出入参标识，IN,OUT,INOUT',
  `data_type` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '参数类型',
  `order` int(11) NOT NULL COMMENT '参数顺序',
  `trans_id` int(11) NULL DEFAULT NULL COMMENT '关联的transform ID',
  `state` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT 'Y' COMMENT '状态，默认Y，N则为不在用',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `trans_id`(`trans_id`) USING BTREE,
  CONSTRAINT `py_procedure_params_ibfk_1` FOREIGN KEY (`trans_id`) REFERENCES `py_transforms` (`id`) ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE = InnoDB AUTO_INCREMENT = 3 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '存储过程参数配置' ROW_FORMAT = DYNAMIC;

-- ----------------------------
-- Table structure for py_trans_type
-- ----------------------------
CREATE TABLE `py_trans_type`  (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  `trans_type_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT 'transform支持的类型名称',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 10 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = DYNAMIC;

-- ----------------------------
-- Records of py_trans_type
-- ----------------------------
INSERT INTO `py_trans_type` (`id`, `trans_type_name`) VALUES (1, 'procedure');
INSERT INTO `py_trans_type` (`id`, `trans_type_name`) VALUES (2, 'sql');
INSERT INTO `py_trans_type` (`id`, `trans_type_name`) VALUES (3, 'transform');
INSERT INTO `py_trans_type` (`id`, `trans_type_name`) VALUES (4, 'table');
INSERT INTO `py_trans_type` (`id`, `trans_type_name`) VALUES (5, 'WebService');
INSERT INTO `py_trans_type` (`id`, `trans_type_name`) VALUES (6, 'javascript');
INSERT INTO `py_trans_type` (`id`, `trans_type_name`) VALUES (7, 'xmlParser');
INSERT INTO `py_trans_type` (`id`, `trans_type_name`) VALUES (8, 'jsonParser');
INSERT INTO `py_trans_type` (`id`, `trans_type_name`) VALUES (9, 'http');
INSERT INTO `py_trans_type` (`id`, `trans_type_name`) VALUES (10, 'list_2_table');


-- ----------------------------
-- Table structure for py_transforms
-- ----------------------------
CREATE TABLE `py_transforms`  (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  `job_id` int(11) NOT NULL COMMENT 'job ID',
  `type_id` int(11) NOT NULL COMMENT 'transform类型，关联:py_trans_type',
  `name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT 'transform名称',
  `from_conn_id` int(11) NULL DEFAULT NULL COMMENT '（目前仅动态SQL取数时需要配置）来源数据库，关联py_connections',
  `from_sql` varchar(10000) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '（目前仅动态SQL取数时需要配置）取数SQL',
  `to_conn_id` int(11) NULL DEFAULT NULL COMMENT '目标库',
  `to_target` varchar(10000) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '目标表，或者目标执行sql',
  `order` int(11) NOT NULL COMMENT '执行顺序',
  `state` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT 'Y' COMMENT '状态，默认Y，N则为不在用',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `job_id`(`job_id`) USING BTREE,
  INDEX `connection_id`(`from_conn_id`) USING BTREE,
  INDEX `to_conn_id`(`to_conn_id`) USING BTREE,
  INDEX `type_id`(`type_id`) USING BTREE,
  CONSTRAINT `py_transforms_ibfk_1` FOREIGN KEY (`from_conn_id`) REFERENCES `py_connections` (`id`) ON DELETE RESTRICT ON UPDATE CASCADE,
  CONSTRAINT `py_transforms_ibfk_2` FOREIGN KEY (`job_id`) REFERENCES `py_jobs` (`id`) ON DELETE RESTRICT ON UPDATE CASCADE,
  CONSTRAINT `py_transforms_ibfk_3` FOREIGN KEY (`to_conn_id`) REFERENCES `py_connections` (`id`) ON DELETE RESTRICT ON UPDATE CASCADE,
  CONSTRAINT `py_transforms_ibfk_4` FOREIGN KEY (`type_id`) REFERENCES `py_trans_type` (`id`) ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE = InnoDB AUTO_INCREMENT = 18 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '转换任务，配置在JOB之后的子流程。如果JOB类型是执行存储过程，则只需要配置job中的source_sql不需要再配置transform表' ROW_FORMAT = DYNAMIC;

-- ----------------------------
-- Table structure for py_variable_cfg
-- ----------------------------
CREATE TABLE `py_variable_cfg`  (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `trans_id` int(11) NOT NULL COMMENT '对应的转换',
  `var_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '变量名称',
  `var_type` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '变量类型',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `fk_trans_id_v`(`trans_id`) USING BTREE,
  CONSTRAINT `fk_trans_id_v` FOREIGN KEY (`trans_id`) REFERENCES `py_transforms` (`id`) ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE = InnoDB AUTO_INCREMENT = 6 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '变量配置表' ROW_FORMAT = Dynamic;



-- 修改pid字段长度
ALTER TABLE `py_etl_run_log` MODIFY COLUMN `pid` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL AFTER `id`;


-- 创建各种日志相关的视图
CREATE OR REPLACE VIEW v_py_jobs AS
SELECT
	`job`.`name` AS `job名称`,
	`job`.`comments` AS `说明`,
	`jt`.`jobType` AS `job类型`,
	`job`.`source_sql` AS `源SQL`,
	`conn`.`db` AS `源数据库`,
	`dbt`.`dbType` AS `源数据库类型`,
	`trt`.`trans_type_name` AS `转换类型`,
	`tr`.`from_sql` AS `转换源SQL`,
	`conn1`.`db` AS `转换源数据库`,
	`tr`.`to_target` AS `转换目标`,
	`conn2`.`db` AS `目标库`,
	`tr`.`order` AS `执行顺序`
FROM
	`py_jobs` `job`
	JOIN `py_jobtype` `jt` ON `job`.`job_type_id` = `jt`.`id`
	JOIN `py_connections` `conn` ON `job`.`source_conn_id` = `conn`.`id`
	JOIN `py_dbtype` `dbt` ON `conn`.`db_type_id` = `dbt`.`id`
	LEFT JOIN `py_transforms` `tr`
	ON `job`.`id` = `tr`.`job_id`
		AND `tr`.`state` = 'Y'
	LEFT JOIN `py_connections` `conn1` ON `tr`.`from_conn_id` = `conn1`.`id`
	LEFT JOIN `py_connections` `conn2` ON `tr`.`to_conn_id` = `conn2`.`id`
	LEFT JOIN `py_trans_type` `trt` ON `tr`.`type_id` = `trt`.`id`
WHERE `job`.`state` = 'Y'
ORDER BY `job`.`id`, `tr`.`order`;

CREATE OR REPLACE VIEW v_py_etl_run_log_job AS
SELECT date_format(`a`.`create_time`, '%Y-%m-%d %H') AS `日期`, `a`.`pid` AS `进程号`, `a`.`job_chain` AS `任务链`
  , min(`a`.`create_time`) AS `开始时间`, max(`a`.`create_time`) AS `结束时间`
  , timestampdiff(SECOND, min(`a`.`create_time`), max(`a`.`create_time`)) AS `耗时`
FROM `py_etl_run_log` `a`
WHERE `a`.`create_time` >= now() - INTERVAL 1 DAY
GROUP BY `a`.`job_chain`, `a`.`pid`, date_format(`a`.`create_time`, '%Y-%m-%d %H')
ORDER BY date_format(`a`.`create_time`, '%Y-%m-%d %H') DESC;


CREATE OR REPLACE VIEW v_py_etl_run_log_sub_job AS
SELECT date_format(`a`.`create_time`, '%Y-%m-%d') AS `日期`, `a`.`current_job` AS `子任务`
  , min(`a`.`create_time`) AS `开始时间`, max(`a`.`create_time`) AS `结束时间`
  , timestampdiff(SECOND, min(`a`.`create_time`), max(`a`.`create_time`)) AS `耗时`
FROM `py_etl_run_log` `a`
WHERE (NOT `a`.`current_job` LIKE '%-%')
  AND `a`.`create_time` >= now() - INTERVAL 1 DAY
GROUP BY `a`.`current_job`, `a`.`pid`, date_format(`a`.`create_time`, '%Y-%m-%d')
ORDER BY `a`.`create_time` DESC;


CREATE OR REPLACE VIEW v_py_etl_run_log_err AS
SELECT
	`o`.`id` AS `id`,
	`o`.`pid` AS `pid`,
	`o`.`job_chain` AS `job_chain`,
	`o`.`current_job` AS `current_job`,
	`o`.`create_time` AS `create_time`,
	`o`.`status` AS `status`,
	`o`.`message` AS `message`,
	`o`.`line_no` AS `line_no`
FROM
	`py_etl_run_log` `o`
WHERE
	(
		`o`.`pid` IN (
		SELECT DISTINCT
			`a`.`pid`
		FROM
			`py_etl_run_log` `a`
		WHERE
		( `a`.`status` <> 'INFO' ) AND `a`.`create_time` >= ( now() - INTERVAL 1 DAY ))
		AND (
		`o`.`create_time` >= ( now() - INTERVAL 1 DAY )))
ORDER BY
	`o`.`create_time` DESC;



-- tag:zpd-pyetl-3.0.7  添加失败继续执行的功能
alter table py_jobs add is_fail_continue varchar(2) default 'N' comment '失败是否继续执行，默认为N，不执行';


-- tag:zpd-pyetl-4.0.0  mile stone：增加普通HTTP、WebService调用功能，增加JSON解析、XML解析功能、增加变量存入数据库能力。

CREATE TABLE `py_data_parser` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  `trans_id` int(11) DEFAULT NULL COMMENT '关联的transform ID',
  `match_path` varchar(255) DEFAULT NULL COMMENT '匹配路径',
  `match_rule` varchar(255) NOT NULL DEFAULT 'common' COMMENT '匹配规则：common',
  `map_field` varchar(255) DEFAULT NULL COMMENT '映射的字段名',
  `comment` varchar(255) NOT NULL COMMENT '参数类型',
  `order` int(11) NOT NULL DEFAULT '0' COMMENT '参数顺序',
  `state` varchar(255) DEFAULT 'Y' COMMENT '状态，默认Y，N则为不在用',
  PRIMARY KEY (`id`) USING BTREE,
  KEY `trans_id` (`trans_id`) USING BTREE,
  CONSTRAINT `py_data_parser_ibfk_1` FOREIGN KEY (`trans_id`) REFERENCES `py_transforms` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=9 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='json/xml解析配置表';


update py_trans_type a set a.trans_type_name = 'WebService' where a.trans_type_name = 'webservice';

COMMIT;

-- 修改日志的字段类型
alter table py_etl_run_log  modify column `message` text  NULL COMMENT '日志信息';

-- tag:zpd-pyetl-4.0.3 添加调用shell脚本功能，job级别配置方式：在py_jobs表中将可执行的shell脚本配置到source_sql字段即可
INSERT INTO py_jobtype (`id`, `jobType`) VALUES (5, 'shell');
-- tag:zpd-pyetl-4.0.3 添加调用shell脚本功能，transform级别配置方式：在py_transforms表中将可执行的shell脚本配置到from_sql字段即可
INSERT INTO py_trans_type(`id`, `trans_type_name`) VALUES (11, 'shell');

-- tag:zpd-pyetl-4.0.4 存储过程配置表的出入参字段：in_out_type 修改为枚举类型，避免配置错误
ALTER TABLE `py_procedure_params` modify column in_out_type ENUM('IN', 'OUT');


-- tag:zpd-pyetl-4.0.5 增加REST服务状态检测，可以在函数中编写逻辑

CREATE TABLE `py_etl_monitor` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  `mornitor_type` varchar(20) DEFAULT NULL COMMENT '监控类型',
  `mornitor_msg` text COMMENT '监控信息',
  `mornitor_value` text COMMENT '监控值',
  `modify_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '监控时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `idx_unq_mornitor_type` (`mornitor_type`)
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;

INSERT INTO `py_etl_monitor` (`mornitor_type`, `mornitor_msg`, `mornitor_value`) VALUES ('rest_status', '服务正常', '0');


CREATE FUNCTION `f_check_rest_status`() RETURNS varchar(10) CHARSET utf8
BEGIN
  DECLARE v_time_diff INT DEFAULT 1;
  DECLARE v_exit VARCHAR(10);

  IF v_time_diff >= 5 THEN
    -- SELECT '存在异常，请检查' ret_msg;
    SET v_exit:= '1';
    UPDATE `py_etl_monitor` a SET a.mornitor_value = v_exit, a.mornitor_msg = 'REST服务存在异常' WHERE a.mornitor_type = 'rest_status';
  ELSE
    SET v_exit:= '0';
    UPDATE `py_etl_monitor` a SET a.mornitor_value = v_exit, a.mornitor_msg = 'REST服务正常' WHERE a.mornitor_type = 'rest_status';
  END IF;

  RETURN v_exit;

END;

-- tag:zpd-pyetl-4.0.7 新增表注释
alter table py_trans_type add `trans_type_comment` varchar(255);
UPDATE `py_trans_type` SET `trans_type_comment` = '调用存储' WHERE `trans_type_name` = 'procedure';
UPDATE `py_trans_type` SET `trans_type_comment` = '调用SQL' WHERE `trans_type_name` = 'sql';
UPDATE `py_trans_type` SET `trans_type_comment` = '如果jobType=syn_dynamic，转换类型选这个' WHERE `trans_type_name` = 'transform';
UPDATE `py_trans_type` SET `trans_type_comment` = '直接入表' WHERE `trans_type_name` = 'table';
UPDATE `py_trans_type` SET `trans_type_comment` = '调用WebService接口' WHERE `trans_type_name` = 'WebService';
UPDATE `py_trans_type` SET `trans_type_comment` = '调用JavaScript代码' WHERE `trans_type_name` = 'javascript';
UPDATE `py_trans_type` SET `trans_type_comment` = 'xml解析' WHERE `trans_type_name` = 'xmlParser';
UPDATE `py_trans_type` SET `trans_type_comment` = 'json解析' WHERE `trans_type_name` = 'jsonParser';
UPDATE `py_trans_type` SET `trans_type_comment` = '调用HTTP接口' WHERE `trans_type_name` = 'http';
UPDATE `py_trans_type` SET `trans_type_comment` = 'xml、json解析后入库' WHERE `trans_type_name` = 'list_2_table';
UPDATE `py_trans_type` SET `trans_type_comment` = '调用shell脚本' WHERE `trans_type_name` = 'shell';

alter table py_jobtype add `jobTypeName` varchar(255);
UPDATE `py_jobtype` SET `jobTypeName` = '表对表同步抽取' WHERE `jobType` = 'syn';
UPDATE `py_jobtype` SET `jobTypeName` = '存储过程、SQL脚本、语句块调用' WHERE `jobType` = 'procedure';
UPDATE `py_jobtype` SET `jobTypeName` = '查询结果轮询处理、常用于回写' WHERE `jobType` = 'stream';
UPDATE `py_jobtype` SET `jobTypeName` = '动态语句抽取，如按病区抽取' WHERE `jobType` = 'syn_dynamic';
UPDATE `py_jobtype` SET `jobTypeName` = '调用shell脚本' WHERE `jobType` = 'shell';


-- tag:zpd-pyetl-4.0.8 新增密码配置错误提醒
CREATE TRIGGER `ist_pwd_check` BEFORE INSERT ON `py_connections` FOR EACH ROW begin
  declare v_pwd varchar(255);
  declare v_msg varchar(255);

  select FROM_BASE64(FROM_BASE64(new.password)) into v_pwd;

  if v_pwd is null then
      set v_msg = concat('密码配置错误：', new.password);
      signal sqlstate '45000' set message_text = v_msg;
  end if;
end;

-- tag:zpd-pyetl-4.0.8 新增密码配置错误提醒
CREATE TRIGGER `upt_pwd_check` BEFORE UPDATE ON `py_connections` FOR EACH ROW begin
  declare v_pwd varchar(255);
  declare v_msg varchar(255);

  select FROM_BASE64(FROM_BASE64(new.password)) into v_pwd;

  if v_pwd is null then
      set v_msg = concat('密码配置错误：', new.password);
      signal sqlstate '45000' set message_text = v_msg;
  end if;
end;

-- tag:zpd-pyetl-4.1.1 增加备库地址配置
alter table py_connections add back_host varchar(255) comment '备库地址';

-- tag:zpd-pyetl-4.1.3 增加BLOB类型同步支持
CREATE TABLE `py_blob_fields` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  `job_id` int(11) DEFAULT NULL COMMENT '关联的Job ID',
  `field_seq` int(11) NOT NULL COMMENT '字段顺序，从0开始',
  `field_name` varchar(255) NOT NULL COMMENT '字段名称',
  `is_blob` int(1) NOT NULL DEFAULT '0' COMMENT '是否BLOB字段：1是，0否',
  `state` varchar(255) DEFAULT 'Y' COMMENT '状态，默认Y，N则为不在用',
  PRIMARY KEY (`id`) USING BTREE,
  KEY `job_id` (`job_id`) USING BTREE,
  CONSTRAINT `py_blob_fields_jobs_ibfk_2` FOREIGN KEY (`job_id`) REFERENCES `py_jobs` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='字段配置';

INSERT INTO `py_jobtype` (`id`, `jobType`) VALUES (6, 'syn_blob');

-- tag:zpd-pyetl-4.1.4 增加支持可插拔的插件调用能力，具备动态调用自定义函数，用于非结构化数据处理。
INSERT INTO `py_trans_type` (`id`, `trans_type_name`, `trans_type_comment`) VALUES (12, 'dy_function', '调用动态函数');

-- tag:zpd-pyetl-5.0.0 增加支持crontab定时任务
CREATE TABLE `py_crontabs`  (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键，无意义',
  `cron` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT 'crontab语法定时任务配置',
  `jobs_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT 'shell脚本、Job名称(多个用逗号隔开)',
  `state` enum('Y','N') CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'Y' COMMENT '状态：默认可用：Y，不可用：N',
  `cron_type` enum('shell','pyjobs') CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '任务类型：shell或者是py_jobs表中配置的任务',
  `comments` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '备注',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 3 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of py_crontabs
-- ----------------------------
INSERT INTO `py_crontabs` VALUES (1, '* * * * *', 'timeout 300s sh /soft/etl/zpd_pyetl_files/slave_check.sh', 'Y', 'shell', '备库监控脚本');
INSERT INTO `py_crontabs` VALUES (2, '*/2 * * * *', 'log_write_back,log_syn', 'Y', 'pyjobs', '快速任务');

-- tag:zpd-pyetl-5.0.2 增加支持crontab定时任务
alter table py_crontabs add
(
  `is_concurrency` enum('Y','N') NOT NULL DEFAULT 'N' COMMENT '是否并发',
  `modify_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- tag:zpd-pyetl-5.0.3 视图方便查询正在同步的数据
CREATE OR REPLACE VIEW `v_py_jobs_syn_view`
AS
SELECT `a`.`id` AS `id`, `a`.`state` AS `state`, `a`.`name` AS `name`, `a`.`source_sql` AS `source_sql`, `a`.`comments` AS `comments`
	, `b`.`to_target` AS `目标表`, `c`.`db` AS `db`, `c`.`db_type_id` AS `db_type_id`, `c`.`host` AS `host`, `c`.`port` AS `port`
	, `c`.`user` AS `user`, `c`.`password` AS `password`
FROM `py_jobs` `a`
	JOIN `py_transforms` `b` ON `a`.`id` = `b`.`job_id`
	JOIN `py_connections` `c` ON `a`.`source_conn_id` = `c`.`id`
WHERE `a`.`job_type_id` = 1
	AND `a`.`state` = 'Y';

-- tag:zpd-pyetl-5.0.4 增加支持db2
INSERT INTO py_dbtype(id, dbType) VALUES ('5', 'db2');

SET FOREIGN_KEY_CHECKS = 1;