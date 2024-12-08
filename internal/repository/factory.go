package repository

import (
	"github.com/flexprice/flexprice/internal/clickhouse"
	"github.com/flexprice/flexprice/internal/domain/auth"
	"github.com/flexprice/flexprice/internal/domain/customer"
	"github.com/flexprice/flexprice/internal/domain/events"
	"github.com/flexprice/flexprice/internal/domain/meter"
	"github.com/flexprice/flexprice/internal/domain/plan"
	"github.com/flexprice/flexprice/internal/domain/price"
	"github.com/flexprice/flexprice/internal/domain/subscription"
	"github.com/flexprice/flexprice/internal/domain/user"
	"github.com/flexprice/flexprice/internal/logger"
	"github.com/flexprice/flexprice/internal/postgres"
	clickhouseRepo "github.com/flexprice/flexprice/internal/repository/clickhouse"
	postgresRepo "github.com/flexprice/flexprice/internal/repository/postgres"
)

type RepositoryType string

const (
	PostgresRepo   RepositoryType = "postgres"
	ClickHouseRepo RepositoryType = "clickhouse"
)

func NewEventRepository(store *clickhouse.ClickHouseStore, logger *logger.Logger) events.Repository {
	return clickhouseRepo.NewEventRepository(store, logger)
}

func NewMeterRepository(db *postgres.DB, logger *logger.Logger) meter.Repository {
	return postgresRepo.NewMeterRepository(db, logger)
}

func NewUserRepository(db *postgres.DB, logger *logger.Logger) user.Repository {
	return postgresRepo.NewUserRepository(db, logger)
}

func NewAuthRepository(db *postgres.DB, logger *logger.Logger) auth.Repository {
	return postgresRepo.NewAuthRepository(db, logger)
}

func NewPriceRepository(db *postgres.DB, logger *logger.Logger) price.Repository {
	return postgresRepo.NewPriceRepository(db, logger)
}

func NewCustomerRepository(db *postgres.DB, logger *logger.Logger) customer.Repository {
	return postgresRepo.NewCustomerRepository(db, logger)
}

func NewPlanRepository(db *postgres.DB, logger *logger.Logger) plan.Repository {
	return postgresRepo.NewPlanRepository(db, logger)
}

func NewSubscriptionRepository(db *postgres.DB, logger *logger.Logger) subscription.Repository {
	return postgresRepo.NewSubscriptionRepository(db, logger)
}
