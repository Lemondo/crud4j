delimiter //

DROP PROCEDURE IF EXISTS `lst_test_table`
//

CREATE PROCEDURE `lst_test_table`(IN `p_empcode` INT(11))
READS SQL DATA
BEGIN
  IF `p_empcode` IS NULL THEN
    SELECT t.`id`
          ,t.`empcode`
          ,t.`loginname`
          ,t.`password`
          ,t.`loginenabled`
      FROM `test_table` t
     WHERE t.`deactivated` = 0;
  ELSE
    SELECT t.`id`
          ,t.`empcode`
          ,t.`loginname`
          ,t.`password`
          ,t.`loginenabled`
      FROM `test_table` t
     WHERE t.`deactivated` = 0
       AND t.`empcode` = `p_empcode`;
  END IF;
END;
//

delimiter ;
