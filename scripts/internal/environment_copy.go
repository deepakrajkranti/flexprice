package internal

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/flexprice/flexprice/ent"
	"github.com/flexprice/flexprice/internal/cache"
	"github.com/flexprice/flexprice/internal/config"
	"github.com/flexprice/flexprice/internal/domain/feature"
	"github.com/flexprice/flexprice/internal/domain/meter"
	"github.com/flexprice/flexprice/internal/domain/plan"
	"github.com/flexprice/flexprice/internal/domain/price"
	"github.com/flexprice/flexprice/internal/logger"
	"github.com/flexprice/flexprice/internal/postgres"
	entRepo "github.com/flexprice/flexprice/internal/repository/ent"
	"github.com/flexprice/flexprice/internal/sentry"
	"github.com/flexprice/flexprice/internal/types"
	"github.com/samber/lo"
)

// CopySummary tracks the statistics for the environment copy operation
type CopySummary struct {
	Features struct {
		Total      int `json:"total"`
		Successful int `json:"successful"`
		Failed     int `json:"failed"`
	} `json:"features"`
	Meters struct {
		Total      int `json:"total"`
		Successful int `json:"successful"`
		Failed     int `json:"failed"`
	} `json:"meters"`
	Plans struct {
		Total      int `json:"total"`
		Successful int `json:"successful"`
		Failed     int `json:"failed"`
	} `json:"plans"`
	Prices struct {
		Total      int `json:"total"`
		Successful int `json:"successful"`
		Failed     int `json:"failed"`
		Skipped    int `json:"skipped"`
	} `json:"prices"`
	FeatureMeterUpdates struct {
		Total      int `json:"total"`
		Successful int `json:"successful"`
		Failed     int `json:"failed"`
	} `json:"feature_meter_updates"`
}

type environmentCopyScript struct {
	cfg         *config.Configuration
	log         *logger.Logger
	featureRepo feature.Repository
	meterRepo   meter.Repository
	planRepo    plan.Repository
	priceRepo   price.Repository
	entClient   *ent.Client
	pgClient    postgres.IClient
	summary     *CopySummary
}

// This script copies features, meters, plans, and prices from one environment to another within the same tenant.

// ## Usage

// ```bash
// go run scripts/main.go -cmd copy-environment -tenant-id="your-tenant-id" -source-environment-id="source-env-id" -target-environment-id="target-env-id" -scope="published"
// ```

// ## Required Parameters

// - `-tenant-id`: The tenant ID for both source and target environments
// - `-source-environment-id`: The environment ID to copy from
// - `-target-environment-id`: The environment ID to copy to
// - `-scope`: Scope for copying - either "published" or "all" (default: "published")

// ## Example

// ```bash
// # Copy only published entities (default)
// go run scripts/main.go -cmd copy-environment \
//   -tenant-id="your-tenant-id" \
//   -source-environment-id="source-env-id" \
//   -target-environment-id="target-env-id" \
//   -scope="published"

// # Copy all entities regardless of status
// go run scripts/main.go -cmd copy-environment \
//   -tenant-id="your-tenant-id" \
//   -source-environment-id="source-env-id" \
//   -target-environment-id="target-env-id" \
//   -scope="all"
// ```

// ## What the script does

// 1. **Copy Features**: Copies all published features from source to target environment
// 2. **Copy Meters**: Copies all published meters from source to target environment
// 3. **Update Feature Meter References**: Updates the meter references in features to point to the new meter IDs
// 4. **Copy Plans**: Copies all published plans from source to target environment
// 5. **Copy Prices**: Copies all published plan-scoped prices from source to target environment, updating plan and meter references

func newEnvironmentCopyScript() (*environmentCopyScript, error) {
	// Load configuration
	cfg, err := config.NewConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	// Initialize logger
	log, err := logger.NewLogger(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create logger: %w", err)
	}

	// Initialize the database client
	entClient, err := postgres.NewEntClient(cfg, log)
	if err != nil {
		log.Fatalf("Failed to connect to postgres: %v", err)
		return nil, err
	}

	// Create postgres client
	pgClient := postgres.NewClient(entClient, log, sentry.NewSentryService(cfg, log))
	cacheClient := cache.NewInMemoryCache()

	// Initialize repositories
	featureRepo := entRepo.NewFeatureRepository(pgClient, log, cacheClient)
	meterRepo := entRepo.NewMeterRepository(pgClient, log, cacheClient)
	planRepo := entRepo.NewPlanRepository(pgClient, log, cacheClient)
	priceRepo := entRepo.NewPriceRepository(pgClient, log, cacheClient)

	return &environmentCopyScript{
		cfg:         cfg,
		log:         log,
		featureRepo: featureRepo,
		meterRepo:   meterRepo,
		planRepo:    planRepo,
		priceRepo:   priceRepo,
		entClient:   entClient,
		pgClient:    pgClient,
	}, nil
}

// CopyEnvironment copies features, meters, plans, and prices from source environment to target environment
func CopyEnvironment() error {
	tenantID := os.Getenv("TENANT_ID")
	sourceEnvID := os.Getenv("SOURCE_ENVIRONMENT_ID")
	targetEnvID := os.Getenv("TARGET_ENVIRONMENT_ID")
	scope := os.Getenv("COPY_SCOPE")

	if tenantID == "" || sourceEnvID == "" || targetEnvID == "" {
		return fmt.Errorf("TENANT_ID, SOURCE_ENVIRONMENT_ID, and TARGET_ENVIRONMENT_ID are required")
	}

	if scope == "" {
		scope = "published" // default to published
	}

	script, err := newEnvironmentCopyScript()
	if err != nil {
		return fmt.Errorf("failed to initialize script: %w", err)
	}

	// Initialize summary
	script.summary = &CopySummary{}

	// Create context with tenant ID
	ctx := context.WithValue(context.Background(), types.CtxTenantID, tenantID)

	script.log.Infow("Starting environment copy",
		"tenant_id", tenantID,
		"source_env_id", sourceEnvID,
		"target_env_id", targetEnvID,
		"scope", scope)

	// Step 1: Copy features
	script.log.Info("Step 1: Copying features...")
	featureMap, err := script.copyFeatures(ctx, sourceEnvID, targetEnvID, scope)
	if err != nil {
		return fmt.Errorf("failed to copy features: %w", err)
	}

	// Step 2: Copy meters
	script.log.Info("Step 2: Copying meters...")
	meterMap, err := script.copyMeters(ctx, sourceEnvID, targetEnvID, scope)
	if err != nil {
		return fmt.Errorf("failed to copy meters: %w", err)
	}

	// Step 2.5: Update feature meter references
	script.log.Info("Step 2.5: Updating feature meter references...")
	err = script.updateFeatureMeterReferences(ctx, targetEnvID, featureMap, meterMap)
	if err != nil {
		return fmt.Errorf("failed to update feature meter references: %w", err)
	}

	// Step 3: Copy plans
	script.log.Info("Step 3: Copying plans...")
	planMap, err := script.copyPlans(ctx, sourceEnvID, targetEnvID, scope)
	if err != nil {
		return fmt.Errorf("failed to copy plans: %w", err)
	}

	// Step 4: Copy prices
	script.log.Info("Step 4: Copying prices...")
	err = script.copyPrices(ctx, sourceEnvID, targetEnvID, scope, planMap, meterMap)
	if err != nil {
		return fmt.Errorf("failed to copy prices: %w", err)
	}

	// Print final summary
	script.printCopySummary()

	script.log.Infow("Environment copy completed successfully",
		"tenant_id", tenantID,
		"source_env_id", sourceEnvID,
		"target_env_id", targetEnvID)

	return nil
}

// copyFeatures copies features from source to target environment
// Returns a map of old feature ID to new feature ID
func (s *environmentCopyScript) copyFeatures(ctx context.Context, sourceEnvID, targetEnvID, scope string) (map[string]string, error) {
	// Create context with source environment
	sourceCtx := context.WithValue(ctx, types.CtxEnvironmentID, sourceEnvID)

	// Get all features from source environment
	featureFilter := types.NewNoLimitFeatureFilter()
	if scope == "published" {
		featureFilter.Status = lo.ToPtr(types.StatusPublished)
	}

	features, err := s.featureRepo.ListAll(sourceCtx, featureFilter)
	if err != nil {
		return nil, fmt.Errorf("failed to list features: %w", err)
	}

	s.summary.Features.Total = len(features)
	s.log.Infow("Found features to copy", "count", len(features))

	featureMap := make(map[string]string)

	// Create context with target environment
	targetCtx := context.WithValue(ctx, types.CtxEnvironmentID, targetEnvID)

	for _, f := range features {
		// Create new feature with new ID
		newFeature := &feature.Feature{
			ID:            types.GenerateUUIDWithPrefix(types.UUID_PREFIX_FEATURE),
			Name:          f.Name,
			LookupKey:     f.LookupKey,
			Description:   f.Description,
			Metadata:      f.Metadata,
			Type:          f.Type,
			UnitSingular:  f.UnitSingular,
			UnitPlural:    f.UnitPlural,
			EnvironmentID: targetEnvID,
			BaseModel: types.BaseModel{
				TenantID:  f.TenantID,
				Status:    types.StatusPublished,
				CreatedAt: time.Now().UTC(),
				UpdatedAt: time.Now().UTC(),
				CreatedBy: types.DefaultUserID,
				UpdatedBy: types.DefaultUserID,
			},
		}

		// Note: We'll update the MeterID after copying meters
		newFeature.MeterID = f.MeterID

		err := s.featureRepo.Create(targetCtx, newFeature)
		if err != nil {
			s.log.Errorw("Failed to create feature", "feature_name", f.Name, "error", err)
			s.summary.Features.Failed++
			continue
		}

		featureMap[f.ID] = newFeature.ID
		s.summary.Features.Successful++
		s.log.Infow("Created feature", "old_id", f.ID, "new_id", newFeature.ID, "name", f.Name)
	}

	s.log.Infow("Features copy completed",
		"total", s.summary.Features.Total,
		"successful", s.summary.Features.Successful,
		"failed", s.summary.Features.Failed)
	return featureMap, nil
}

// copyMeters copies meters from source to target environment
// Returns a map of old meter ID to new meter ID
func (s *environmentCopyScript) copyMeters(ctx context.Context, sourceEnvID, targetEnvID, scope string) (map[string]string, error) {
	// Create context with source environment
	sourceCtx := context.WithValue(ctx, types.CtxEnvironmentID, sourceEnvID)

	// Get all meters from source environment
	meterFilter := types.NewNoLimitMeterFilter()
	if scope == "published" {
		meterFilter.Status = lo.ToPtr(types.StatusPublished)
	}

	meters, err := s.meterRepo.ListAll(sourceCtx, meterFilter)
	if err != nil {
		return nil, fmt.Errorf("failed to list meters: %w", err)
	}

	s.summary.Meters.Total = len(meters)
	s.log.Infow("Found meters to copy", "count", len(meters))

	meterMap := make(map[string]string)

	// Create context with target environment
	targetCtx := context.WithValue(ctx, types.CtxEnvironmentID, targetEnvID)

	for _, m := range meters {
		// Create new meter with new ID
		newMeter := &meter.Meter{
			ID:            types.GenerateUUIDWithPrefix(types.UUID_PREFIX_METER),
			EventName:     m.EventName,
			Name:          m.Name,
			Aggregation:   m.Aggregation,
			Filters:       m.Filters,
			ResetUsage:    m.ResetUsage,
			EnvironmentID: targetEnvID,
			BaseModel: types.BaseModel{
				TenantID:  m.TenantID,
				Status:    types.StatusPublished,
				CreatedAt: time.Now().UTC(),
				UpdatedAt: time.Now().UTC(),
				CreatedBy: types.DefaultUserID,
				UpdatedBy: types.DefaultUserID,
			},
		}

		err := s.meterRepo.CreateMeter(targetCtx, newMeter)
		if err != nil {
			s.log.Errorw("Failed to create meter", "meter_name", m.Name, "error", err)
			s.summary.Meters.Failed++
			continue
		}

		meterMap[m.ID] = newMeter.ID
		s.summary.Meters.Successful++
		s.log.Infow("Created meter", "old_id", m.ID, "new_id", newMeter.ID, "name", m.Name)
	}

	s.log.Infow("Meters copy completed",
		"total", s.summary.Meters.Total,
		"successful", s.summary.Meters.Successful,
		"failed", s.summary.Meters.Failed)
	return meterMap, nil
}

// copyPlans copies plans from source to target environment
// Returns a map of old plan ID to new plan ID
func (s *environmentCopyScript) copyPlans(ctx context.Context, sourceEnvID, targetEnvID, scope string) (map[string]string, error) {
	// Create context with source environment
	sourceCtx := context.WithValue(ctx, types.CtxEnvironmentID, sourceEnvID)

	// Get all plans from source environment
	planFilter := types.NewNoLimitPlanFilter()
	if scope == "published" {
		planFilter.Status = lo.ToPtr(types.StatusPublished)
	}

	plans, err := s.planRepo.ListAll(sourceCtx, planFilter)
	if err != nil {
		return nil, fmt.Errorf("failed to list plans: %w", err)
	}

	s.summary.Plans.Total = len(plans)
	s.log.Infow("Found plans to copy", "count", len(plans))

	planMap := make(map[string]string)

	// Create context with target environment
	targetCtx := context.WithValue(ctx, types.CtxEnvironmentID, targetEnvID)

	for _, p := range plans {
		// Create new plan with new ID
		newPlan := &plan.Plan{
			ID:            types.GenerateUUIDWithPrefix(types.UUID_PREFIX_PLAN),
			Name:          p.Name,
			LookupKey:     p.LookupKey,
			Description:   p.Description,
			EnvironmentID: targetEnvID,
			BaseModel: types.BaseModel{
				TenantID:  p.TenantID,
				Status:    types.StatusPublished,
				CreatedAt: time.Now().UTC(),
				UpdatedAt: time.Now().UTC(),
				CreatedBy: types.DefaultUserID,
				UpdatedBy: types.DefaultUserID,
			},
		}

		err := s.planRepo.Create(targetCtx, newPlan)
		if err != nil {
			s.log.Errorw("Failed to create plan", "plan_name", p.Name, "error", err)
			s.summary.Plans.Failed++
			continue
		}

		planMap[p.ID] = newPlan.ID
		s.summary.Plans.Successful++
		s.log.Infow("Created plan", "old_id", p.ID, "new_id", newPlan.ID, "name", p.Name)
	}

	s.log.Infow("Plans copy completed",
		"total", s.summary.Plans.Total,
		"successful", s.summary.Plans.Successful,
		"failed", s.summary.Plans.Failed)
	return planMap, nil
}

// copyPrices copies prices from source to target environment
func (s *environmentCopyScript) copyPrices(ctx context.Context, sourceEnvID, targetEnvID, scope string, planMap, meterMap map[string]string) error {
	// Create context with source environment
	sourceCtx := context.WithValue(ctx, types.CtxEnvironmentID, sourceEnvID)

	// Get all prices from source environment
	priceFilter := types.NewNoLimitPriceFilter()
	if scope == "published" {
		priceFilter.Status = lo.ToPtr(types.StatusPublished)
	}

	prices, err := s.priceRepo.ListAll(sourceCtx, priceFilter)
	if err != nil {
		return fmt.Errorf("failed to list prices: %w", err)
	}

	s.summary.Prices.Total = len(prices)
	s.log.Infow("Found prices to copy", "count", len(prices))

	// Create context with target environment
	targetCtx := context.WithValue(ctx, types.CtxEnvironmentID, targetEnvID)

	var newPrices []*price.Price

	for _, p := range prices {
		// Check if we have the corresponding plan and meter in the target environment
		newPlanID, planExists := planMap[p.PlanID]
		if !planExists {
			s.log.Warnw("Skipping price - plan not found in target environment",
				"price_id", p.ID, "plan_id", p.PlanID)
			s.summary.Prices.Skipped++
			continue
		}

		// Create new price with new ID
		newPrice := &price.Price{
			ID:                     types.GenerateUUIDWithPrefix(types.UUID_PREFIX_PRICE),
			Amount:                 p.Amount,
			DisplayAmount:          p.DisplayAmount,
			Currency:               p.Currency,
			PriceUnitType:          p.PriceUnitType,
			PriceUnitID:            p.PriceUnitID,
			PriceUnitAmount:        p.PriceUnitAmount,
			DisplayPriceUnitAmount: p.DisplayPriceUnitAmount,
			PriceUnit:              p.PriceUnit,
			ConversionRate:         p.ConversionRate,
			PlanID:                 newPlanID,
			Type:                   p.Type,
			BillingPeriod:          p.BillingPeriod,
			BillingPeriodCount:     p.BillingPeriodCount,
			BillingModel:           p.BillingModel,
			BillingCadence:         p.BillingCadence,
			InvoiceCadence:         p.InvoiceCadence,
			TrialPeriod:            p.TrialPeriod,
			TierMode:               p.TierMode,
			Tiers:                  p.Tiers,
			PriceUnitTiers:         p.PriceUnitTiers,
			LookupKey:              p.LookupKey,
			Description:            p.Description,
			TransformQuantity:      p.TransformQuantity,
			Metadata:               p.Metadata,
			EnvironmentID:          targetEnvID,
			Scope:                  p.Scope,
			BaseModel: types.BaseModel{
				TenantID:  p.TenantID,
				Status:    types.StatusPublished,
				CreatedAt: time.Now().UTC(),
				UpdatedAt: time.Now().UTC(),
				CreatedBy: types.DefaultUserID,
				UpdatedBy: types.DefaultUserID,
			},
		}

		// Update meter ID if this price has a meter
		if p.MeterID != "" {
			if newMeterID, meterExists := meterMap[p.MeterID]; meterExists {
				newPrice.MeterID = newMeterID
			} else {
				s.log.Warnw("Skipping price - meter not found in target environment",
					"price_id", p.ID, "meter_id", p.MeterID)
				s.summary.Prices.Skipped++
				continue
			}
		}

		newPrices = append(newPrices, newPrice)
		s.log.Infow("Prepared price for creation", "old_id", p.ID, "new_id", newPrice.ID, "plan_id", newPlanID)
	}

	// Create prices in bulk
	if len(newPrices) > 0 {
		err = s.priceRepo.CreateBulk(targetCtx, newPrices)
		if err != nil {
			s.summary.Prices.Failed = len(newPrices)
			return fmt.Errorf("failed to create prices in bulk: %w", err)
		}
		s.summary.Prices.Successful = len(newPrices)
	}

	s.log.Infow("Prices copy completed",
		"total", s.summary.Prices.Total,
		"successful", s.summary.Prices.Successful,
		"failed", s.summary.Prices.Failed,
		"skipped", s.summary.Prices.Skipped)
	return nil
}

// updateFeatureMeterReferences updates the meter references in features after copying meters
func (s *environmentCopyScript) updateFeatureMeterReferences(ctx context.Context, targetEnvID string, featureMap, meterMap map[string]string) error {
	// Create context with target environment
	targetCtx := context.WithValue(ctx, types.CtxEnvironmentID, targetEnvID)

	// Get all features in target environment
	featureFilter := types.NewNoLimitFeatureFilter()
	featureFilter.Status = lo.ToPtr(types.StatusPublished)

	features, err := s.featureRepo.ListAll(targetCtx, featureFilter)
	if err != nil {
		return fmt.Errorf("failed to list features: %w", err)
	}

	s.summary.FeatureMeterUpdates.Total = len(features)
	updatedCount := 0
	for _, f := range features {
		// Check if this feature has a meter ID that needs to be updated
		if f.MeterID != "" {
			if newMeterID, exists := meterMap[f.MeterID]; exists {
				// Update the feature with new meter ID
				f.MeterID = newMeterID
				f.UpdatedAt = time.Now().UTC()
				f.UpdatedBy = types.DefaultUserID

				err := s.featureRepo.Update(targetCtx, f)
				if err != nil {
					s.log.Errorw("Failed to update feature meter reference", "feature_id", f.ID, "error", err)
					s.summary.FeatureMeterUpdates.Failed++
					continue
				}

				updatedCount++
				s.summary.FeatureMeterUpdates.Successful++
				s.log.Infow("Updated feature meter reference", "feature_id", f.ID, "old_meter_id", f.MeterID, "new_meter_id", newMeterID)
			}
		}
	}

	s.log.Infow("Feature meter references update completed",
		"total", s.summary.FeatureMeterUpdates.Total,
		"successful", s.summary.FeatureMeterUpdates.Successful,
		"failed", s.summary.FeatureMeterUpdates.Failed)
	return nil
}

// printCopySummary prints a comprehensive summary of the copy operation
func (s *environmentCopyScript) printCopySummary() {
	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("ENVIRONMENT COPY SUMMARY")
	fmt.Println(strings.Repeat("=", 80))

	fmt.Printf("Features:     %d total, %d successful, %d failed\n",
		s.summary.Features.Total, s.summary.Features.Successful, s.summary.Features.Failed)

	fmt.Printf("Meters:       %d total, %d successful, %d failed\n",
		s.summary.Meters.Total, s.summary.Meters.Successful, s.summary.Meters.Failed)

	fmt.Printf("Plans:        %d total, %d successful, %d failed\n",
		s.summary.Plans.Total, s.summary.Plans.Successful, s.summary.Plans.Failed)

	fmt.Printf("Prices:       %d total, %d successful, %d failed, %d skipped\n",
		s.summary.Prices.Total, s.summary.Prices.Successful, s.summary.Prices.Failed, s.summary.Prices.Skipped)

	fmt.Printf("Meter Updates: %d total, %d successful, %d failed\n",
		s.summary.FeatureMeterUpdates.Total, s.summary.FeatureMeterUpdates.Successful, s.summary.FeatureMeterUpdates.Failed)

	// Calculate totals
	totalProcessed := s.summary.Features.Total + s.summary.Meters.Total + s.summary.Plans.Total + s.summary.Prices.Total
	totalSuccessful := s.summary.Features.Successful + s.summary.Meters.Successful + s.summary.Plans.Successful + s.summary.Prices.Successful
	totalFailed := s.summary.Features.Failed + s.summary.Meters.Failed + s.summary.Plans.Failed + s.summary.Prices.Failed

	fmt.Println(strings.Repeat("-", 80))
	fmt.Printf("OVERALL:      %d total items processed\n", totalProcessed)
	fmt.Printf("              %d successfully copied\n", totalSuccessful)
	fmt.Printf("              %d failed to copy\n", totalFailed)
	fmt.Printf("              %d prices skipped (missing dependencies)\n", s.summary.Prices.Skipped)

	if totalFailed > 0 {
		fmt.Println("\n⚠️  Some items failed to copy. Check the logs above for details.")
	} else {
		fmt.Println("\n✅ All items copied successfully!")
	}

	fmt.Println(strings.Repeat("=", 80))
}
