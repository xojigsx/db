package qp

import (
	"context"
	"log/slog"

	"github.com/jmoiron/sqlx"
	sqldblogger "github.com/simukti/sqldb-logger"

	_ "github.com/go-sql-driver/mysql" // register mysql driver
	_ "github.com/lib/pq"              // register postgres driver
)

func Open(driver, dsn string) (*sqlx.DB, error) {
	db, err := sqlx.Connect(driver, dsn)
	if err != nil {
		return nil, err
	}

	db.DB = sqldblogger.OpenDriver(dsn, db.DB.Driver(), dblogger)

	if err = db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}

	return db, nil
}

var dblogger sqldblogger.Logger = slogadapter{}

type slogadapter struct{}

func (slogadapter) Log(ctx context.Context, level sqldblogger.Level, msg string, data map[string]interface{}) {
	var slogLevel slog.Level

	switch level {
	case sqldblogger.LevelTrace:
		slogLevel = slog.LevelDebug
	case sqldblogger.LevelDebug:
		slogLevel = slog.LevelInfo
	case sqldblogger.LevelInfo:
		slogLevel = slog.LevelWarn
	case sqldblogger.LevelError:
		slogLevel = slog.LevelError
	}

	attrs := make([]slog.Attr, 0, len(data))
	for k, v := range data {
		attrs = append(attrs, slog.Any(k, v))
	}

	slog.LogAttrs(ctx, slogLevel, msg, attrs...)
}
