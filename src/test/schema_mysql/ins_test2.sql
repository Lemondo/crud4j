delimiter //

DROP PROCEDURE IF EXISTS `ins_test2`
//

CREATE FUNCTION `ins_test2`(`p_data` VARCHAR(1000))
  RETURNS INT(11)
MODIFIES SQL DATA
BEGIN
  DECLARE `v_id` TINYINT;

  INSERT INTO `test2` (`data`)
               VALUES (`p_data`);

  SELECT LAST_INSERT_ID() INTO `v_id`;

  RETURN `v_id`;
END;
//

delimiter ;
