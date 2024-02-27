CREATE TABLE IF NOT EXISTS metric (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT UNIQUE NOT NULL,
  type TEXT NOT NULL,
  help TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS label_value (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  label TEXT NOT NULL,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS series (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  metric_id INTEGER NOT NULL REFERENCES metric(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS label_set (
  label_value_id INTEGER NOT NULL REFERENCES label_value(id),
  series_id INTEGER NOT NULL REFERENCES series(id) ON DELETE CASCADE,
  PRIMARY KEY (label_value_id, series_id)
);

CREATE VIEW IF NOT EXISTS label_set_view AS
  SELECT ls.series_id, GROUP_CONCAT(CONCAT(lv.label, '="', lv.value, '"'), ', ') as label_set
  FROM label_set ls
  INNER JOIN label_value lv ON lv.id = ls.label_value_id
  GROUP BY ls.series_id;

CREATE VIEW IF NOT EXISTS series_view AS
  SELECT s.id, CONCAT(m.name, '{', ls.label_set, '}') as name
  FROM series s
  INNER JOIN metric m ON m.id = s.metric_id
  INNER JOIN label_set_view ls ON ls.series_id = s.id;