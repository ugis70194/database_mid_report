CREATE TABLE soup(
  item_name VARCHAR(256),
  price DECIMAL(16, 2) NOT NULL,
  PRIMARY KEY(item_name)
);

INSERT INTO soup VALUES
  ('豆腐の味噌汁', 100),
  ('コンソメスープ', 100),
  ('豚汁', 200);