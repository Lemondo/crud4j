CREATE TABLE `test_table` (
  `id`           varchar(36) NOT NULL,
  `empcode`      int(11),
  `loginname`    varchar(30),
  `password`     varchar(30),
  `loginenabled` varchar(1),
  `deactivated`  tinyint(1) NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`)
);