package service

import (
	"context"
	"fmt"
	"time"

	"github.com/flexprice/flexprice/internal/api/dto"
	"github.com/flexprice/flexprice/internal/domain/plan"
	"github.com/flexprice/flexprice/internal/domain/price"
	"github.com/flexprice/flexprice/internal/domain/subscription"
	"github.com/flexprice/flexprice/internal/logger"
	"github.com/flexprice/flexprice/internal/types"
)

type SubscriptionService interface {
	CreateSubscription(ctx context.Context, req dto.CreateSubscriptionRequest) (*dto.SubscriptionResponse, error)
	GetSubscription(ctx context.Context, id string) (*dto.SubscriptionResponse, error)
	CancelSubscription(ctx context.Context, id string, cancelAtPeriodEnd bool) error
	ListSubscriptions(ctx context.Context, filter *types.SubscriptionFilter) (*dto.ListSubscriptionsResponse, error)
}

type subscriptionService struct {
	subscriptionRepo subscription.Repository
	planRepo         plan.Repository
	priceRepo        price.Repository
	logger           *logger.Logger
}

func NewSubscriptionService(
	subscriptionRepo subscription.Repository,
	planRepo plan.Repository,
	priceRepo price.Repository,
	logger *logger.Logger,
) SubscriptionService {
	return &subscriptionService{
		subscriptionRepo: subscriptionRepo,
		planRepo:         planRepo,
		priceRepo:        priceRepo,
		logger:           logger,
	}
}

func (s *subscriptionService) CreateSubscription(ctx context.Context, req dto.CreateSubscriptionRequest) (*dto.SubscriptionResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	subscription := req.ToSubscription(ctx)
	if err := s.subscriptionRepo.Create(ctx, subscription); err != nil {
		return nil, fmt.Errorf("failed to create subscription: %w", err)
	}

	return &dto.SubscriptionResponse{Subscription: subscription}, nil
}

func (s *subscriptionService) GetSubscription(ctx context.Context, id string) (*dto.SubscriptionResponse, error) {
	subscription, err := s.subscriptionRepo.Get(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("failed to get subscription: %w", err)
	}

	// TODO: Think of a better way to handle this initialization of other services
	planService := NewPlanService(s.planRepo, s.priceRepo, s.logger)

	plan, err := planService.GetPlan(ctx, subscription.PlanID)
	if err != nil {
		return nil, fmt.Errorf("failed to get plan: %w", err)
	}

	return &dto.SubscriptionResponse{Subscription: subscription, Plan: plan}, nil
}

func (s *subscriptionService) CancelSubscription(ctx context.Context, id string, cancelAtPeriodEnd bool) error {
	subscription, err := s.subscriptionRepo.Get(ctx, id)
	if err != nil {
		return fmt.Errorf("failed to get subscription: %w", err)
	}

	subscription.SubscriptionStatus = types.SubscriptionStatusCancelled
	subscription.Status = types.StatusPublished
	subscription.CancelledAt = &time.Time{}
	subscription.CancelAtPeriodEnd = cancelAtPeriodEnd
	subscription.UpdatedAt = time.Now().UTC()
	subscription.UpdatedBy = types.GetUserID(ctx)

	if err := s.subscriptionRepo.Update(ctx, subscription); err != nil {
		return fmt.Errorf("failed to cancel subscription: %w", err)
	}

	return nil
}

func (s *subscriptionService) ListSubscriptions(ctx context.Context, filter *types.SubscriptionFilter) (*dto.ListSubscriptionsResponse, error) {
	if filter.Limit == 0 {
		filter.Limit = 10
	}

	subscriptions, err := s.subscriptionRepo.List(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("failed to list subscriptions: %w", err)
	}

	response := &dto.ListSubscriptionsResponse{
		Subscriptions: make([]*dto.SubscriptionResponse, len(subscriptions)),
		Total:         len(subscriptions),
		Offset:        filter.Offset,
		Limit:         filter.Limit,
	}

	for i, sub := range subscriptions {
		response.Subscriptions[i] = &dto.SubscriptionResponse{
			Subscription: sub,
		}
	}

	return response, nil
}
