CREATE TABLE request(
  branch VARCHAR(256) NOT NULL, 
  request_datetime DATETIME NOT NULL,
  request_index MEDIUMINT UNSIGNED NOT NULL, 
  request_user_id VARCHAR(17) NOT NULL,
  rice VARCHAR(256) NOT NULL, 
  soup VARCHAR(256) NOT NULL, 
  salad VARCHAR(256) NOT NULL,
  main_dish VARCHAR(256) NOT NULL,
  total_price DECIMAL(16, 2) NOT NULL,
  PRIMARY KEY(branch, request_datetime, request_index)
);