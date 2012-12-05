delimiter //

DROP PROCEDURE IF EXISTS `lst_test_table`
//

CREATE PROCEDURE `lst_test_table`(IN `p_empcode` INT(11))
READS SQL DATA
BEGIN
  IF `p_empcode` IS NULL THEN
    SELECT t.`id` AS `id`
          ,t.`empcode` AS `employee`
          ,t.`loginname` AS `login`
          ,t.`password` AS `password`
          ,t.`loginenabled` AS `loginenabled`
      FROM `test_table` t
     WHERE t.`deactivated` = 0;
  ELSE
    SELECT t.`id` AS `id`
          ,t.`empcode` AS `employee`
          ,t.`loginname` AS `login`
          ,t.`password` AS `password`
          ,t.`loginenabled` AS `loginenabled`
      FROM `test_table` t
     WHERE t.`deactivated` = 0
       AND t.`empcode` = `p_empcode`;
  END IF;
END;
//

delimiter ;
