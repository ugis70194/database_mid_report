CREATE TABLE rice(
  item_name VARCHAR(256) NOT NULL,
  price DECIMAL(16, 2) NOT NULL,
  PRIMARY KEY(item_name)
);

INSERT INTO rice VALUES
  ('白米 小', 150),
  ('白米 中', 200),
  ('白米 大', 250);