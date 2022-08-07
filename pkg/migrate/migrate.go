package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	constant "github.com/NpoolPlatform/go-service-framework/pkg/mysql/const"

	billingconst "github.com/NpoolPlatform/cloud-hashing-billing/pkg/message/const"
	orderconst "github.com/NpoolPlatform/cloud-hashing-order/pkg/message/const"

	"github.com/NpoolPlatform/go-service-framework/pkg/config"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"

	_ "github.com/go-sql-driver/mysql" // nolint
)

const (
	keyUsername = "username"
	keyPassword = "password"
	keyDBName   = "database_name"
)

func dsn(hostname string) (string, error) {
	username := config.GetStringValueWithNameSpace(constant.MysqlServiceName, keyUsername)
	password := config.GetStringValueWithNameSpace(constant.MysqlServiceName, keyPassword)
	dbname := config.GetStringValueWithNameSpace(hostname, keyDBName)

	svc, err := config.PeekService(constant.MysqlServiceName)
	if err != nil {
		logger.Sugar().Warnw("dsb", "error", err)
		return "", err
	}

	return fmt.Sprintf("%v:%v@tcp(%v:%v)/%v?parseTime=true&interpolateParams=true",
		username, password,
		svc.Address,
		svc.Port,
		dbname,
	), nil
}

func open(hostname string) (conn *sql.DB, err error) {
	hdsn, err := dsn(hostname)
	if err != nil {
		return nil, err
	}

	conn, err = sql.Open("mysql", hdsn)
	if err != nil {
		return nil, err
	}

	// https://github.com/go-sql-driver/mysql
	// See "Important settings" section.
	conn.SetConnMaxLifetime(time.Minute * 3)
	conn.SetMaxOpenConns(10)
	conn.SetMaxIdleConns(10)

	return conn, nil
}

func Migrate(ctx context.Context) error {
	_, err := open(orderconst.ServiceName)
	if err != nil {
		return err
	}

	_, err = open(billingconst.ServiceName)
	if err != nil {
		return err
	}

	// Prepare mysql instant for order / billing / ledger
	// Migrate payments to ledger details and general
	// Migrate commission to ledger detail and general

	return nil
}
