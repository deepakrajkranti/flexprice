package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/flexprice/flexprice/internal/domain/price"
	"github.com/flexprice/flexprice/internal/logger"
	"github.com/flexprice/flexprice/internal/postgres"
	"github.com/flexprice/flexprice/internal/types"
)

type priceRepository struct {
	db     *postgres.DB
	logger *logger.Logger
}

func NewPriceRepository(db *postgres.DB, logger *logger.Logger) price.Repository {
	return &priceRepository{db: db, logger: logger}
}

func (r *priceRepository) Create(ctx context.Context, price *price.Price) error {
	price.DisplayAmount = price.GetDisplayAmount()
	query := `
		INSERT INTO prices (
			id, tenant_id, amount, display_amount, currency, plan_id, type, 
			billing_period, billing_period_count, billing_model, billing_cadence, 
			tier_mode, tiers, meter_id, filter_values, transform, lookup_key, description,
			metadata, status, created_at, updated_at, created_by, updated_by
		) VALUES (
			:id, :tenant_id, :amount, :display_amount, :currency, :plan_id, :type,
			:billing_period, :billing_period_count, :billing_model, :billing_cadence,
			:tier_mode, :tiers, :meter_id, :filter_values, :transform, :lookup_key,
			:description, :metadata, :status, :created_at, :updated_at, :created_by, :updated_by
		)`

	r.logger.Debug("creating price ",
		"price_id ", price.ID,
		"tenant_id ", price.TenantID,
	)

	_, err := r.db.NamedExecContext(ctx, query, price)
	if err != nil {
		return fmt.Errorf("failed to insert price: %w", err)
	}

	return nil
}

func (r *priceRepository) Get(ctx context.Context, id string) (*price.Price, error) {
	var p price.Price
	query := `
		SELECT * FROM prices 
		WHERE id = :id 
		AND tenant_id = :tenant_id
		AND status = :status`

	rows, err := r.db.NamedQueryContext(ctx, query, map[string]interface{}{
		"id":        id,
		"tenant_id": types.GetTenantID(ctx),
		"status":    types.StatusPublished,
	})

	// TODO: Handle not found error better to not throw 500
	if err != nil {
		return nil, fmt.Errorf("failed to get price: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, fmt.Errorf("price not found")
	}

	if err := rows.StructScan(&p); err != nil {
		return nil, fmt.Errorf("failed to scan price: %w", err)
	}
	return &p, nil
}

func (r *priceRepository) GetByPlanID(ctx context.Context, planID string) ([]*price.Price, error) {
	var prices []*price.Price
	query := `
		SELECT * FROM prices
		WHERE plan_id = :plan_id
		AND tenant_id = :tenant_id
		AND status = :status`

	rows, err := r.db.NamedQueryContext(ctx, query, map[string]interface{}{
		"plan_id":   planID,
		"tenant_id": types.GetTenantID(ctx),
		"status":    types.StatusPublished,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get prices: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var p price.Price
		if err := rows.StructScan(&p); err != nil {
			return nil, fmt.Errorf("failed to scan price: %w", err)
		}
		prices = append(prices, &p)
	}

	return prices, nil
}

func (r *priceRepository) List(ctx context.Context, filter types.Filter) ([]*price.Price, error) {
	var prices []*price.Price
	query := `
		SELECT * FROM prices 
		WHERE tenant_id = :tenant_id 
		AND status = :status
		ORDER BY created_at DESC 
		LIMIT :limit OFFSET :offset`

	// First, prepare the named query
	rows, err := r.db.NamedQueryContext(ctx, query, map[string]interface{}{
		"tenant_id": types.GetTenantID(ctx),
		"status":    types.StatusPublished,
		"limit":     filter.Limit,
		"offset":    filter.Offset,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	// Iterate through the rows and scan into price objects
	for rows.Next() {
		var p price.Price
		if err := rows.StructScan(&p); err != nil {
			return nil, fmt.Errorf("failed to scan price: %w", err)
		}
		prices = append(prices, &p)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return prices, nil
}

func (r *priceRepository) Update(ctx context.Context, price *price.Price) error {
	price.UpdatedAt = time.Now().UTC()
	price.UpdatedBy = types.GetUserID(ctx)

	query := `
		UPDATE prices SET 
			lookup_key = :lookup_key,
			description = :description,
			metadata = :metadata,
			status = :status,
			updated_at = :updated_at,
			updated_by = :updated_by
		WHERE id = :id 
		AND tenant_id = :tenant_id`

	result, err := r.db.NamedExecContext(ctx, query, price)
	if err != nil {
		return fmt.Errorf("failed to update price: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("price not found")
	}
	return nil
}

func (r *priceRepository) Delete(ctx context.Context, id string) error {
	query := `
		UPDATE prices SET 
			status = :status,
			updated_at = :updated_at,
			updated_by = :updated_by
		WHERE id = :id 
		AND tenant_id = :tenant_id`

	_, err := r.db.NamedExecContext(ctx, query, map[string]interface{}{
		"id":         id,
		"tenant_id":  types.GetTenantID(ctx),
		"status":     types.StatusDeleted,
		"updated_at": time.Now(),
		"updated_by": types.GetUserID(ctx),
	})
	if err != nil {
		return fmt.Errorf("failed to update price status: %w", err)
	}
	return nil
}
