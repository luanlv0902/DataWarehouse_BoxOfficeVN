/*
 Navicat Premium Dump SQL

 Source Server         : MySQL
 Source Server Type    : MySQL
 Source Server Version : 100432 (10.4.32-MariaDB)
 Source Host           : localhost:3306
 Source Schema         : db_control

 Target Server Type    : MySQL
 Target Server Version : 100432 (10.4.32-MariaDB)
 File Encoding         : 65001

 Date: 22/11/2025 21:54:42
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for etl_config
-- ----------------------------
DROP TABLE IF EXISTS `etl_config`;
CREATE TABLE `etl_config`  (
  `config_key` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `config_value` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `description` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL,
  `updated_at` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`config_key`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of etl_config
-- ----------------------------
INSERT INTO `etl_config` VALUES ('aggregate_data_path', 'data/aggregate', 'Thư mục lưu trữ dữ liệu tổng hợp', '2025-11-22 20:14:42');
INSERT INTO `etl_config` VALUES ('cleaned_data_path', 'data/cleaned', 'Thư mục lưu trữ dữ liệu đã làm sạch', '2025-11-22 20:14:42');
INSERT INTO `etl_config` VALUES ('raw_data_path', 'data/raw', 'Thư mục lưu trữ dữ liệu thô', '2025-11-22 20:14:42');

-- ----------------------------
-- Table structure for etl_log
-- ----------------------------
DROP TABLE IF EXISTS `etl_log`;
CREATE TABLE `etl_log`  (
  `id` int NOT NULL AUTO_INCREMENT,
  `log_time` datetime NOT NULL,
  `log_level` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
  `message` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
  `source_file` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = DYNAMIC;

-- ----------------------------
-- Records of etl_log
-- ----------------------------

SET FOREIGN_KEY_CHECKS = 1;
