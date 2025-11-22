/*
 Navicat Premium Dump SQL

 Source Server         : MySQL
 Source Server Type    : MySQL
 Source Server Version : 100432 (10.4.32-MariaDB)
 Source Host           : localhost:3306
 Source Schema         : db_datamart

 Target Server Type    : MySQL
 Target Server Version : 100432 (10.4.32-MariaDB)
 File Encoding         : 65001

 Date: 22/11/2025 21:54:53
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for dm_daily_revenue
-- ----------------------------
DROP TABLE IF EXISTS `dm_daily_revenue`;
CREATE TABLE `dm_daily_revenue`  (
  `id` int NOT NULL AUTO_INCREMENT,
  `movie_name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `full_date` date NULL DEFAULT NULL,
  `revenue_vnd` bigint NULL DEFAULT NULL,
  `tickets_sold` int NULL DEFAULT NULL,
  `showtimes` int NULL DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of dm_daily_revenue
-- ----------------------------

-- ----------------------------
-- Table structure for dm_top_movies
-- ----------------------------
DROP TABLE IF EXISTS `dm_top_movies`;
CREATE TABLE `dm_top_movies`  (
  `id` int NOT NULL AUTO_INCREMENT,
  `movie_name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `total_revenue` bigint NULL DEFAULT NULL,
  `total_tickets` int NULL DEFAULT NULL,
  `total_showtimes` int NULL DEFAULT NULL,
  `ranking` int NULL DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of dm_top_movies
-- ----------------------------

SET FOREIGN_KEY_CHECKS = 1;
