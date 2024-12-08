package plan

import (
	"github.com/flexprice/flexprice/internal/types"
)

type Plan struct {
	ID             string               `db:"id" json:"id"`
	Name           string               `db:"name" json:"name"`
	LookupKey      string               `db:"lookup_key" json:"lookup_key"`
	Description    string               `db:"description" json:"description"`
	InvoiceCadence types.InvoiceCadence `db:"invoice_cadence" json:"invoice_cadence"`
	TrialPeriod    int                  `db:"trial_period" json:"trial_period"`
	types.BaseModel
}
