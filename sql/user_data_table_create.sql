CREATE TABLE user_data(
  user_id VARCHAR(17),
  user_name VARCHAR(256) NOT NULL,
  tel VARCHAR(16) NOT NULL,
  postal_number VARCHAR(16) NOT NULL,
  ship_address VARCHAR(256) NOT NULL,
  card_number VARCHAR(24) NOT NULL,
  PRIMARY KEY(user_id)
);

INSERT INTO user_data VALUES
  ('S0000000000000001', '小金井支店', '0424721835', '184-0002','東京都小金井市梶尾町5丁目1-1', ''),
  ('S0000000000000002', '三鷹支店'  , '0428692759', '181-0013','東京都三鷹市下連雀3丁目46-1' , ''),
  ('S0000000000000003', '国分寺支店', '0421856983', '185-0012','東京都国分寺市本町2丁目1-23' , '');