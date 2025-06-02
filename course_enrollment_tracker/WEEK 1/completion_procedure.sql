USE course_tracker;

DELIMITER $$

CREATE PROCEDURE CalculateCompletion(IN stu_id INT, IN crs_id INT, OUT completion FLOAT)
BEGIN
    SELECT completion_percentage INTO completion
    FROM progress
    WHERE student_id = stu_id AND course_id = crs_id;

    IF completion IS NULL THEN
        SET completion = 0;
    END IF;
END$$

DELIMITER ;
CALL CalculateCompletion(1, 1, @completion);
SELECT @completion;
