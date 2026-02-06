-- Cleanup existing tables
-- We don't have IF EXISTS, so we ignore errors manually if running via client
-- but for this file we assume they might exist or we just want to be sure.
DROP TABLE comments;
DROP TABLE blog_posts;
DROP TABLE users;

-- Create tables with relations
CREATE TABLE users (id INT, name VARCHAR, email VARCHAR);
CREATE TABLE blog_posts (id INT, author_id INT, title VARCHAR, content VARCHAR);
CREATE TABLE comments (id INT, post_id INT, author_id INT, text VARCHAR);

-- Populate users
INSERT INTO users VALUES (1, 'Fabio', 'fabio@example.com');
INSERT INTO users VALUES (2, 'John', 'john@example.com');
INSERT INTO users VALUES (3, 'Alice', 'alice@example.com');

-- Populate blog posts
INSERT INTO blog_posts VALUES (1, 1, 'First Post', 'Hello world');
INSERT INTO blog_posts VALUES (2, 1, 'Second Post', 'ThunderDB is cool');
INSERT INTO blog_posts VALUES (3, 2, 'Johns Post', 'I am John');

-- Populate comments
INSERT INTO comments VALUES (1, 1, 2, 'Nice post!');
INSERT INTO comments VALUES (2, 1, 3, 'Agreed');
INSERT INTO comments VALUES (3, 2, 2, 'Great work');

-- Verify data integrity and relationships using SQL queries
SELECT * FROM users;
SELECT * FROM blog_posts WHERE author_id = 1;
SELECT * FROM comments WHERE post_id = 1;

-- CRUD: UPDATE
UPDATE users SET name = 'Fabio Updated' WHERE id = 1;
SELECT * FROM users WHERE id = 1;

-- CRUD: DELETE
DELETE FROM comments WHERE id = 3;
SELECT * FROM comments;

-- Final check of all tables
SHOW TABLES;
SELECT * FROM users;
SELECT * FROM blog_posts;
SELECT * FROM comments;

-- Test special commands (now supported in client)
.schema users
.schema blog_posts
.stats comments
