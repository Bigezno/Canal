
CREATE TABLE `t_test_user` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `name` varchar(64) COLLATE utf8mb4_general_ci NOT NULL COMMENT '姓名',
  `age` int NOT NULL COMMENT '年龄',
  `c_time` datetime NOT NULL,
  `del_flag` tinyint NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=9 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='测试用户表';



CREATE TABLE `t_test_role` (
  `id` int NOT NULL AUTO_INCREMENT,
  `role_name` varchar(32) COLLATE utf8mb4_general_ci NOT NULL,
  `del_flag` tinyint NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='测试角色表';



CREATE TABLE `t_test_tag` (
  `id` bigint NOT NULL AUTO_INCREMENT COMMENT '主键',
  `name` varchar(32) COLLATE utf8mb4_general_ci NOT NULL COMMENT '标签名称',
  `del_flag` tinyint NOT NULL COMMENT '逻辑删除0-正常;1-删除;',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='测试标签';


CREATE TABLE `t_test_user_role` (
  `id` bigint NOT NULL AUTO_INCREMENT COMMENT '主键',
  `user_id` bigint NOT NULL COMMENT '用户id',
  `role_id` bigint NOT NULL COMMENT '角色id',
  `del_flag` tinyint NOT NULL COMMENT '逻辑删除:0-正常,1-存在',
  PRIMARY KEY (`id`),
  KEY `idx_role_id` (`role_id`),
  KEY `idx_user_id` (`user_id`)
) ENGINE=InnoDB AUTO_INCREMENT=11 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='用户-角色关联表';


CREATE TABLE `t_test_user_tag` (
  `id` bigint NOT NULL AUTO_INCREMENT COMMENT '主键',
  `user_id` bigint NOT NULL COMMENT '用户id',
  `tag_id` bigint NOT NULL COMMENT '标签id',
  `c_time` bigint NOT NULL COMMENT '创建时间',
  `del_flag` tinyint DEFAULT NULL COMMENT '逻辑删除0-正常;1-删除;',
  PRIMARY KEY (`id`),
  KEY `idx_tag_id` (`tag_id`),
  KEY `idx_user_id` (`user_id`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='用户标签';



