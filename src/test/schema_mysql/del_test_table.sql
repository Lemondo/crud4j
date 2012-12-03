delimiter //

DROP PROCEDURE IF EXISTS `del_test_table`
//

CREATE FUNCTION `del_test_table`(`p_id` VARCHAR(36))
  RETURNS TINYINT(1)
MODIFIES SQL DATA
BEGIN
  DECLARE `rowcount` TINYINT;

  DELETE FROM `test_table`
   WHERE `id` = `p_id`;

  SELECT ROW_COUNT() INTO `rowcount`;

  RETURN `rowcount`;
END;
//

delimiter ;
