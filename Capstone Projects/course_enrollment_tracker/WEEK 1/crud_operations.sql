USE course_tracker;

-- Insert new student
INSERT INTO students (name, email, registration_date) VALUES ('Alice', 'alice@example.com', CURDATE());

-- Insert new course
INSERT INTO courses (course_name, description) VALUES ('Python Basics', 'Intro to Python programming.');

-- New enrollment
INSERT INTO enrollments (student_id, course_id, enrollment_date) VALUES (1, 1, CURDATE());

-- Update progress (Insert or update)
INSERT INTO progress (student_id, course_id, completion_percentage, last_updated)
VALUES (1, 1, 25, CURDATE())
ON DUPLICATE KEY UPDATE completion_percentage = 25, last_updated = CURDATE();

-- Select progress for student 1 in course 1
SELECT p.completion_percentage, p.last_updated
FROM progress p
WHERE p.student_id = 1 AND p.course_id = 1;

-- Delete enrollment
DELETE FROM enrollments WHERE student_id = 1 AND course_id = 1;
