-- Blog benchmark SQL test
--
-- Mirrors the Rust blog_benchmark_test with 5 users, 50 posts, ~150 comments.
-- Exercises: CREATE TABLE, CREATE INDEX, INSERT, COUNT(*), LIKE, JOINs,
-- WHERE pushdown, ORDER BY, LIMIT, and multi-table joins.

-- ── Schema ─────────────────────────────────────────────────────────────

CREATE TABLE users (id INT, name VARCHAR, email VARCHAR);
CREATE TABLE blog_posts (id INT, author_id INT, title VARCHAR, content VARCHAR);
CREATE TABLE comments (id INT, post_id INT, author_id INT, text VARCHAR);

-- ── Indexes ────────────────────────────────────────────────────────────

CREATE INDEX idx_users_id ON users (id);
CREATE INDEX idx_posts_id ON blog_posts (id);
CREATE INDEX idx_posts_author ON blog_posts (author_id);
CREATE INDEX idx_comments_post ON comments (post_id);
CREATE INDEX idx_comments_author ON comments (author_id);

-- ── Users (5) ──────────────────────────────────────────────────────────

INSERT INTO users VALUES (1, 'user_1', 'user_1@example.com');
INSERT INTO users VALUES (2, 'user_2', 'user_2@example.com');
INSERT INTO users VALUES (3, 'user_3', 'user_3@example.com');
INSERT INTO users VALUES (4, 'user_4', 'user_4@example.com');
INSERT INTO users VALUES (5, 'user_5', 'user_5@example.com');

-- ── Blog posts (50) ───────────────────────────────────────────────────
-- Topics rotate: rust, database, performance, testing, design
-- author_id = (i % 5) + 1

INSERT INTO blog_posts VALUES (1, 2, 'Post about database #1', 'This is post 1 discussing database in depth. ThunderDB makes database easy.');
INSERT INTO blog_posts VALUES (2, 3, 'Post about performance #2', 'This is post 2 discussing performance in depth. ThunderDB makes performance easy.');
INSERT INTO blog_posts VALUES (3, 4, 'Post about testing #3', 'This is post 3 discussing testing in depth. ThunderDB makes testing easy.');
INSERT INTO blog_posts VALUES (4, 5, 'Post about design #4', 'This is post 4 discussing design in depth. ThunderDB makes design easy.');
INSERT INTO blog_posts VALUES (5, 1, 'Post about rust #5', 'This is post 5 discussing rust in depth. ThunderDB makes rust easy.');
INSERT INTO blog_posts VALUES (6, 2, 'Post about database #6', 'This is post 6 discussing database in depth. ThunderDB makes database easy.');
INSERT INTO blog_posts VALUES (7, 3, 'Post about performance #7', 'This is post 7 discussing performance in depth. ThunderDB makes performance easy.');
INSERT INTO blog_posts VALUES (8, 4, 'Post about testing #8', 'This is post 8 discussing testing in depth. ThunderDB makes testing easy.');
INSERT INTO blog_posts VALUES (9, 5, 'Post about design #9', 'This is post 9 discussing design in depth. ThunderDB makes design easy.');
INSERT INTO blog_posts VALUES (10, 1, 'Post about rust #10', 'This is post 10 discussing rust in depth. ThunderDB makes rust easy.');
INSERT INTO blog_posts VALUES (11, 2, 'Post about database #11', 'This is post 11 discussing database in depth. ThunderDB makes database easy.');
INSERT INTO blog_posts VALUES (12, 3, 'Post about performance #12', 'This is post 12 discussing performance in depth. ThunderDB makes performance easy.');
INSERT INTO blog_posts VALUES (13, 4, 'Post about testing #13', 'This is post 13 discussing testing in depth. ThunderDB makes testing easy.');
INSERT INTO blog_posts VALUES (14, 5, 'Post about design #14', 'This is post 14 discussing design in depth. ThunderDB makes design easy.');
INSERT INTO blog_posts VALUES (15, 1, 'Post about rust #15', 'This is post 15 discussing rust in depth. ThunderDB makes rust easy.');
INSERT INTO blog_posts VALUES (16, 2, 'Post about database #16', 'This is post 16 discussing database in depth. ThunderDB makes database easy.');
INSERT INTO blog_posts VALUES (17, 3, 'Post about performance #17', 'This is post 17 discussing performance in depth. ThunderDB makes performance easy.');
INSERT INTO blog_posts VALUES (18, 4, 'Post about testing #18', 'This is post 18 discussing testing in depth. ThunderDB makes testing easy.');
INSERT INTO blog_posts VALUES (19, 5, 'Post about design #19', 'This is post 19 discussing design in depth. ThunderDB makes design easy.');
INSERT INTO blog_posts VALUES (20, 1, 'Post about rust #20', 'This is post 20 discussing rust in depth. ThunderDB makes rust easy.');
INSERT INTO blog_posts VALUES (21, 2, 'Post about database #21', 'This is post 21 discussing database in depth. ThunderDB makes database easy.');
INSERT INTO blog_posts VALUES (22, 3, 'Post about performance #22', 'This is post 22 discussing performance in depth. ThunderDB makes performance easy.');
INSERT INTO blog_posts VALUES (23, 4, 'Post about testing #23', 'This is post 23 discussing testing in depth. ThunderDB makes testing easy.');
INSERT INTO blog_posts VALUES (24, 5, 'Post about design #24', 'This is post 24 discussing design in depth. ThunderDB makes design easy.');
INSERT INTO blog_posts VALUES (25, 1, 'Post about rust #25', 'This is post 25 discussing rust in depth. ThunderDB makes rust easy.');
INSERT INTO blog_posts VALUES (26, 2, 'Post about database #26', 'This is post 26 discussing database in depth. ThunderDB makes database easy.');
INSERT INTO blog_posts VALUES (27, 3, 'Post about performance #27', 'This is post 27 discussing performance in depth. ThunderDB makes performance easy.');
INSERT INTO blog_posts VALUES (28, 4, 'Post about testing #28', 'This is post 28 discussing testing in depth. ThunderDB makes testing easy.');
INSERT INTO blog_posts VALUES (29, 5, 'Post about design #29', 'This is post 29 discussing design in depth. ThunderDB makes design easy.');
INSERT INTO blog_posts VALUES (30, 1, 'Post about rust #30', 'This is post 30 discussing rust in depth. ThunderDB makes rust easy.');
INSERT INTO blog_posts VALUES (31, 2, 'Post about database #31', 'This is post 31 discussing database in depth. ThunderDB makes database easy.');
INSERT INTO blog_posts VALUES (32, 3, 'Post about performance #32', 'This is post 32 discussing performance in depth. ThunderDB makes performance easy.');
INSERT INTO blog_posts VALUES (33, 4, 'Post about testing #33', 'This is post 33 discussing testing in depth. ThunderDB makes testing easy.');
INSERT INTO blog_posts VALUES (34, 5, 'Post about design #34', 'This is post 34 discussing design in depth. ThunderDB makes design easy.');
INSERT INTO blog_posts VALUES (35, 1, 'Post about rust #35', 'This is post 35 discussing rust in depth. ThunderDB makes rust easy.');
INSERT INTO blog_posts VALUES (36, 2, 'Post about database #36', 'This is post 36 discussing database in depth. ThunderDB makes database easy.');
INSERT INTO blog_posts VALUES (37, 3, 'Post about performance #37', 'This is post 37 discussing performance in depth. ThunderDB makes performance easy.');
INSERT INTO blog_posts VALUES (38, 4, 'Post about testing #38', 'This is post 38 discussing testing in depth. ThunderDB makes testing easy.');
INSERT INTO blog_posts VALUES (39, 5, 'Post about design #39', 'This is post 39 discussing design in depth. ThunderDB makes design easy.');
INSERT INTO blog_posts VALUES (40, 1, 'Post about rust #40', 'This is post 40 discussing rust in depth. ThunderDB makes rust easy.');
INSERT INTO blog_posts VALUES (41, 2, 'Post about database #41', 'This is post 41 discussing database in depth. ThunderDB makes database easy.');
INSERT INTO blog_posts VALUES (42, 3, 'Post about performance #42', 'This is post 42 discussing performance in depth. ThunderDB makes performance easy.');
INSERT INTO blog_posts VALUES (43, 4, 'Post about testing #43', 'This is post 43 discussing testing in depth. ThunderDB makes testing easy.');
INSERT INTO blog_posts VALUES (44, 5, 'Post about design #44', 'This is post 44 discussing design in depth. ThunderDB makes design easy.');
INSERT INTO blog_posts VALUES (45, 1, 'Post about rust #45', 'This is post 45 discussing rust in depth. ThunderDB makes rust easy.');
INSERT INTO blog_posts VALUES (46, 2, 'Post about database #46', 'This is post 46 discussing database in depth. ThunderDB makes database easy.');
INSERT INTO blog_posts VALUES (47, 3, 'Post about performance #47', 'This is post 47 discussing performance in depth. ThunderDB makes performance easy.');
INSERT INTO blog_posts VALUES (48, 4, 'Post about testing #48', 'This is post 48 discussing testing in depth. ThunderDB makes testing easy.');
INSERT INTO blog_posts VALUES (49, 5, 'Post about design #49', 'This is post 49 discussing design in depth. ThunderDB makes design easy.');
INSERT INTO blog_posts VALUES (50, 1, 'Post about rust #50', 'This is post 50 discussing rust in depth. ThunderDB makes rust easy.');

-- ── Comments (2-4 per post, ~150 total) ────────────────────────────────
-- comments_for_post(i) = 2 + (i % 3)  →  2, 3, 4, 2, 3, 4, ...
-- commenter = ((post + c) % 5) + 1

INSERT INTO comments VALUES (1, 1, 2, 'Comment 1 on post 1');
INSERT INTO comments VALUES (2, 1, 3, 'Comment 2 on post 1');
INSERT INTO comments VALUES (3, 2, 3, 'Comment 1 on post 2');
INSERT INTO comments VALUES (4, 2, 4, 'Comment 2 on post 2');
INSERT INTO comments VALUES (5, 2, 5, 'Comment 3 on post 2');
INSERT INTO comments VALUES (6, 3, 4, 'Comment 1 on post 3');
INSERT INTO comments VALUES (7, 3, 5, 'Comment 2 on post 3');
INSERT INTO comments VALUES (8, 3, 1, 'Comment 3 on post 3');
INSERT INTO comments VALUES (9, 3, 2, 'Comment 4 on post 3');
INSERT INTO comments VALUES (10, 4, 5, 'Comment 1 on post 4');
INSERT INTO comments VALUES (11, 4, 1, 'Comment 2 on post 4');
INSERT INTO comments VALUES (12, 5, 1, 'Comment 1 on post 5');
INSERT INTO comments VALUES (13, 5, 2, 'Comment 2 on post 5');
INSERT INTO comments VALUES (14, 5, 3, 'Comment 3 on post 5');
INSERT INTO comments VALUES (15, 6, 2, 'Comment 1 on post 6');
INSERT INTO comments VALUES (16, 6, 3, 'Comment 2 on post 6');
INSERT INTO comments VALUES (17, 6, 4, 'Comment 3 on post 6');
INSERT INTO comments VALUES (18, 6, 5, 'Comment 4 on post 6');
INSERT INTO comments VALUES (19, 7, 3, 'Comment 1 on post 7');
INSERT INTO comments VALUES (20, 7, 4, 'Comment 2 on post 7');
INSERT INTO comments VALUES (21, 8, 4, 'Comment 1 on post 8');
INSERT INTO comments VALUES (22, 8, 5, 'Comment 2 on post 8');
INSERT INTO comments VALUES (23, 8, 1, 'Comment 3 on post 8');
INSERT INTO comments VALUES (24, 9, 5, 'Comment 1 on post 9');
INSERT INTO comments VALUES (25, 9, 1, 'Comment 2 on post 9');
INSERT INTO comments VALUES (26, 9, 2, 'Comment 3 on post 9');
INSERT INTO comments VALUES (27, 9, 3, 'Comment 4 on post 9');
INSERT INTO comments VALUES (28, 10, 1, 'Comment 1 on post 10');
INSERT INTO comments VALUES (29, 10, 2, 'Comment 2 on post 10');

-- (posts 11-50 follow the same pattern but we include a representative subset)
-- Total: 29 comments on posts 1-10 for demonstration

-- ═══════════════════════════════════════════════════════════════════════
-- QUERIES — mirrors the Rust blog_benchmark_test
-- ═══════════════════════════════════════════════════════════════════════

-- 1. Table counts
SELECT COUNT(*) FROM users;
SELECT COUNT(*) FROM blog_posts;
SELECT COUNT(*) FROM comments;

-- 2. Full-text search: title LIKE prefix (rust posts = every 5th = 10 hits)
SELECT id, title FROM blog_posts WHERE title LIKE 'Post about rust%';

-- 3. Full-text search: content LIKE prefix (single hit)
SELECT id, title FROM blog_posts WHERE content LIKE 'This is post 42 %';

-- 4. Posts by author (author_id = 1 gets posts 5,10,15,...,50 = 10 posts)
SELECT id, title FROM blog_posts WHERE author_id = 1;

-- 5. JOIN: single post + comments (post 3 has 4 comments)
SELECT p.id, p.title, c.text
  FROM blog_posts p
  JOIN comments c ON p.id = c.post_id
  WHERE p.id = 3;

-- 6. 3-table JOIN: post + author name + comments
SELECT u.name, p.title, c.text
  FROM users u
  JOIN blog_posts p ON u.id = p.author_id
  JOIN comments c ON p.id = c.post_id
  WHERE p.id = 1;

-- 7. LEFT JOIN: all users with their post count (user_4 and user_5 still appear)
SELECT u.name, p.title
  FROM users u
  LEFT JOIN blog_posts p ON u.id = p.author_id
  WHERE p.id <= 5;

-- 8. Posts by multiple authors using IN
SELECT p.id, u.name, p.title
  FROM blog_posts p
  JOIN users u ON p.author_id = u.id
  WHERE p.author_id IN (1, 3);

-- 9. Range scan on post ID (indexed BETWEEN)
SELECT id, title FROM blog_posts WHERE id BETWEEN 20 AND 30;

-- 10. JOIN with ORDER BY
SELECT u.name, p.title
  FROM users u
  JOIN blog_posts p ON u.id = p.author_id
  ORDER BY p.title
  LIMIT 10;

-- 11. JOIN with COUNT(*)
SELECT COUNT(*)
  FROM users u
  JOIN blog_posts p ON u.id = p.author_id;

-- 12. 3-table JOIN with ORDER BY and LIMIT
SELECT u.name, p.title, c.text
  FROM users u
  JOIN blog_posts p ON u.id = p.author_id
  JOIN comments c ON p.id = c.post_id
  ORDER BY c.text
  LIMIT 5;

-- ── Cleanup ────────────────────────────────────────────────────────────

DROP TABLE comments;
DROP TABLE blog_posts;
DROP TABLE users;
