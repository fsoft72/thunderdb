-- JOIN smoke test

CREATE TABLE users (id INT, name VARCHAR, age INT);
INSERT INTO users VALUES (1, 'Alice', 30);
INSERT INTO users VALUES (2, 'Bob', 25);
INSERT INTO users VALUES (3, 'Charlie', 35);

CREATE TABLE posts (id INT, author_id INT, title VARCHAR);
INSERT INTO posts VALUES (1, 1, 'Post A');
INSERT INTO posts VALUES (2, 1, 'Post B');
INSERT INTO posts VALUES (3, 2, 'Post C');

CREATE TABLE comments (id INT, post_id INT, text VARCHAR);
INSERT INTO comments VALUES (1, 1, 'Nice!');
INSERT INTO comments VALUES (2, 1, 'Great!');
INSERT INTO comments VALUES (3, 3, 'Cool!');

CREATE INDEX idx_users_id ON users (id);
CREATE INDEX idx_posts_author ON posts (author_id);
CREATE INDEX idx_posts_id ON posts (id);
CREATE INDEX idx_comments_post ON comments (post_id);

-- INNER JOIN (3 rows: Alice x2, Bob x1)
SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.author_id;

-- LEFT JOIN (4 rows: +Charlie with NULL)
SELECT u.name, p.title FROM users u LEFT JOIN posts p ON u.id = p.author_id;

-- RIGHT JOIN (3 rows, same as INNER since all posts have authors)
SELECT u.name, p.title FROM users u RIGHT JOIN posts p ON u.id = p.author_id;

-- Multi-table (3 rows: Alice/Post A/Nice!, Alice/Post A/Great!, Bob/Post C/Cool!)
SELECT u.name, p.title, c.text FROM users u JOIN posts p ON u.id = p.author_id JOIN comments c ON p.id = c.post_id;

-- WHERE pushdown (2 rows: Alice's posts)
SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.author_id WHERE u.age > 28;

-- COUNT with JOIN
SELECT COUNT(*) FROM users u JOIN posts p ON u.id = p.author_id;

-- SELECT * from JOIN
SELECT * FROM users u JOIN posts p ON u.id = p.author_id;

DROP TABLE comments;
DROP TABLE posts;
DROP TABLE users;
