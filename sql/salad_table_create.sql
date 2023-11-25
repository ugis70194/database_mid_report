CREATE TABLE salad(
  item_name VARCHAR(256),
  price DECIMAL(16, 2) NOT NULL,
  PRIMARY KEY(item_name)
);

INSERT INTO salad VALUES
  ('ワカメのサラダ', 300),
  ('ほうれん草のごま和え', 200),
  ('チキンのサラダ', 400);