package orders

import (
	"errors"
	"testing"
)

func TestCreateParamsValidate(t *testing.T) {
	valid := func() CreateParams {
		return CreateParams{
			CustomerID:   "cust-1",
			PaymentToken: "tok_abc",
			Items:        []Item{{ProductID: "sku-keyboard", Quantity: 1}},
		}
	}
	tests := []struct {
		name   string
		modify func(*CreateParams)
		ok     bool
	}{
		{"valid", func(*CreateParams) {}, true},
		{"missing customer", func(p *CreateParams) { p.CustomerID = " " }, false},
		{"card number instead of token", func(p *CreateParams) { p.PaymentToken = "4242424242424242" }, false},
		{"no items", func(p *CreateParams) { p.Items = nil }, false},
		{"blank product", func(p *CreateParams) { p.Items[0].ProductID = "" }, false},
		{"zero quantity", func(p *CreateParams) { p.Items[0].Quantity = 0 }, false},
		{"quantity too large", func(p *CreateParams) { p.Items[0].Quantity = maxQuantity + 1 }, false},
		{"duplicate product", func(p *CreateParams) { p.Items = append(p.Items, Item{ProductID: "sku-keyboard", Quantity: 2}) }, false},
		{"too many items", func(p *CreateParams) {
			for range maxItems {
				p.Items = append(p.Items, Item{ProductID: "x", Quantity: 1})
			}
		}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := valid()
			tt.modify(&p)
			err := p.validate()
			if tt.ok && err != nil {
				t.Errorf("validate() = %v, want nil", err)
			}
			if !tt.ok && !errors.Is(err, ErrInvalid) {
				t.Errorf("validate() = %v, want an ErrInvalid error", err)
			}
		})
	}
}
