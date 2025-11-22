/*
 Navicat Premium Dump SQL

 Source Server         : MySQL
 Source Server Type    : MySQL
 Source Server Version : 100432 (10.4.32-MariaDB)
 Source Host           : localhost:3306
 Source Schema         : db_warehouse

 Target Server Type    : MySQL
 Target Server Version : 100432 (10.4.32-MariaDB)
 File Encoding         : 65001

 Date: 22/11/2025 21:55:09
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for dim_date
-- ----------------------------
DROP TABLE IF EXISTS `dim_date`;
CREATE TABLE `dim_date`  (
  `date_key` int NOT NULL,
  `full_date` date NULL DEFAULT NULL,
  `year` int NULL DEFAULT NULL,
  `month` int NULL DEFAULT NULL,
  `day` int NULL DEFAULT NULL,
  `quarter` int NULL DEFAULT NULL,
  PRIMARY KEY (`date_key`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of dim_date
-- ----------------------------

-- ----------------------------
-- Table structure for dim_movie
-- ----------------------------
DROP TABLE IF EXISTS `dim_movie`;
CREATE TABLE `dim_movie`  (
  `movie_key` int NOT NULL AUTO_INCREMENT,
  `movie_name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `genre` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `release_date` date NULL DEFAULT NULL,
  `country` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  PRIMARY KEY (`movie_key`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of dim_movie
-- ----------------------------

-- ----------------------------
-- Table structure for fact_revenue
-- ----------------------------
DROP TABLE IF EXISTS `fact_revenue`;
CREATE TABLE `fact_revenue`  (
  `revenue_id` int NOT NULL AUTO_INCREMENT,
  `movie_key` int NULL DEFAULT NULL,
  `date_key` int NULL DEFAULT NULL,
  `revenue_vnd` bigint NULL DEFAULT NULL,
  `tickets_sold` int NULL DEFAULT NULL,
  `showtimes` int NULL DEFAULT NULL,
  `load_date` datetime NULL DEFAULT NULL,
  PRIMARY KEY (`revenue_id`) USING BTREE,
  INDEX `movie_key`(`movie_key` ASC) USING BTREE,
  INDEX `date_key`(`date_key` ASC) USING BTREE,
  CONSTRAINT `fact_revenue_ibfk_1` FOREIGN KEY (`movie_key`) REFERENCES `dim_movie` (`movie_key`) ON DELETE RESTRICT ON UPDATE RESTRICT,
  CONSTRAINT `fact_revenue_ibfk_2` FOREIGN KEY (`date_key`) REFERENCES `dim_date` (`date_key`) ON DELETE RESTRICT ON UPDATE RESTRICT
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of fact_revenue
-- ----------------------------

SET FOREIGN_KEY_CHECKS = 1;
