delimiter //

DROP PROCEDURE IF EXISTS `ins_test_table`
//

CREATE PROCEDURE `ins_test_table`(IN `p_id` VARCHAR(36)
                                 ,IN `p_empcode` INT(11)
                                 ,IN `p_loginname` VARCHAR(30)
                                 ,IN `p_password` VARCHAR(30)
                                 ,IN `p_loginenabled` VARCHAR(1))
WRITES SQL DATA
BEGIN
  INSERT INTO `test_table` (`id`,`empcode`,`loginname`,`password`,`loginenabled`)
                    VALUES (`p_id`,`p_empcode`,`p_loginname`,`p_password`,`p_loginenabled`);
END;
//

delimiter ;
