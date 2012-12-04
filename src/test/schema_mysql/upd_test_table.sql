delimiter //

DROP PROCEDURE IF EXISTS `upd_test_table`
//

CREATE FUNCTION `upd_test_table`(`p_id` VARCHAR(36)
                                 ,`p_empcode` INT(11)
                                 ,`p_loginname` VARCHAR(30)
                                 ,`p_password` VARCHAR(30)
                                 ,`p_loginenabled` VARCHAR(1))
  RETURNS TINYINT(1)
MODIFIES SQL DATA
BEGIN
  DECLARE `rowcount` TINYINT;

  UPDATE `test_table`
     SET `empcode` = `p_empcode`
        ,`loginname` = `p_loginname`
        ,`password` = `p_password`
        ,`loginenabled` = `p_loginenabled`
   WHERE `id` = `p_id`;

  SELECT ROW_COUNT() INTO `rowcount`;

  RETURN `rowcount`;
END;
//

delimiter ;
