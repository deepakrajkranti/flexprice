package dto

import (
	"context"
	"time"

	"github.com/flexprice/flexprice/internal/domain/subscription"
	"github.com/flexprice/flexprice/internal/types"
	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
)

type CreateSubscriptionRequest struct {
	CustomerID     string               `json:"customer_id" validate:"required"`
	PlanID         string               `json:"plan_id" validate:"required"`
	Currency       string               `json:"currency" validate:"required,len=3"`
	LookupKey      string               `json:"lookup_key"`
	StartDate      time.Time            `json:"start_date,omitempty"`
	EndDate        *time.Time           `json:"end_date,omitempty"`
	TrialStart     *time.Time           `json:"trial_start,omitempty"`
	TrialEnd       *time.Time           `json:"trial_end,omitempty"`
	InvoiceCadence types.InvoiceCadence `json:"invoice_cadence,omitempty"`
}

type UpdateSubscriptionRequest struct {
	Status            types.SubscriptionStatus `json:"status"`
	CancelAt          *time.Time               `json:"cancel_at,omitempty"`
	CancelAtPeriodEnd bool                     `json:"cancel_at_period_end,omitempty"`
}

type SubscriptionResponse struct {
	*subscription.Subscription
	Plan *PlanResponse `json:"plan"`
}

type ListSubscriptionsResponse struct {
	Subscriptions []*SubscriptionResponse `json:"subscriptions"`
	Total         int                     `json:"total"`
	Offset        int                     `json:"offset"`
	Limit         int                     `json:"limit"`
}

func (r *CreateSubscriptionRequest) Validate() error {
	return validator.New().Struct(r)
}

func (r *CreateSubscriptionRequest) ToSubscription(ctx context.Context) *subscription.Subscription {
	now := time.Now().UTC()
	if r.StartDate.IsZero() {
		r.StartDate = now
	}

	return &subscription.Subscription{
		ID:                 uuid.New().String(),
		CustomerID:         r.CustomerID,
		PlanID:             r.PlanID,
		Currency:           r.Currency,
		LookupKey:          r.LookupKey,
		SubscriptionStatus: types.SubscriptionStatusActive,
		StartDate:          r.StartDate,
		EndDate:            r.EndDate,
		TrialStart:         r.TrialStart,
		TrialEnd:           r.TrialEnd,
		InvoiceCadence:     r.InvoiceCadence,
		BillingAnchor:      r.StartDate,
		BaseModel: types.BaseModel{
			TenantID:  types.GetTenantID(ctx),
			CreatedAt: now,
			UpdatedAt: now,
			CreatedBy: types.GetUserID(ctx),
			UpdatedBy: types.GetUserID(ctx),
			Status:    types.StatusPublished,
		},
	}
}
