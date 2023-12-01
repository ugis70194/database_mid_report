CREATE TABLE main_dish(
  item_name VARCHAR(256),
  price DECIMAL(16, 2) NOT NULL,
  PRIMARY KEY(item_name)
);

INSERT INTO main_dish VALUES
  ('鯖の塩焼き', 600),
  ('豚の生姜焼き', 500),
  ('煮込みハンバーグ', 600);