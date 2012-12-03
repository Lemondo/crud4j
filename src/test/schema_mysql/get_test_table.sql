delimiter //

DROP PROCEDURE IF EXISTS `get_test_table`
//

CREATE PROCEDURE `get_test_table`(IN `p_id` VARCHAR(36))
READS SQL DATA
BEGIN
  SELECT t.`id`
        ,t.`empcode`
        ,t.`loginname`
        ,t.`password`
        ,t.`loginenabled`
    FROM `test_table` t
   WHERE t.`id` = `p_id`
     AND t.`deactivated` = 0;
END;
//

delimiter ;
