

-- ec_enterprise_legal 的 id_card需要加解密
ALTER TABLE `ec_enterprise_legal`
MODIFY COLUMN `id_card`  varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL COMMENT '证件号码' AFTER `card_type`;

-- ec_enterprise_operator 的 id_card需要加解密
ALTER TABLE `ec_enterprise_operator`
MODIFY COLUMN `id_card`  varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL COMMENT '证件号码' AFTER `card_type`;

-- ec_enterprise_financing_info 的 reason字段加长
ALTER TABLE `ec_enterprise_financing_info`
MODIFY COLUMN `reason`  varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL DEFAULT NULL COMMENT '终止理由' AFTER `area_site_id`;

