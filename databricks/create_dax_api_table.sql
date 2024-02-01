CREATE TABLE dax_api (
  broker_id INT,
  user_id INT,
  symbol VARCHAR(50),
  ask DECIMAL(18, 8),
  baseVolume24h DECIMAL(18, 8),
  bid DECIMAL(18, 8),
  high24h DECIMAL(18, 8),
  lastPrice DECIMAL(18, 8),
  low24h DECIMAL(18, 8),
  open24h DECIMAL(18, 8),
  quoteVolume24h DECIMAL(18, 8),
  timestamp DATETIME
);