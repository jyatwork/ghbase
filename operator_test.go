package ghbase

import (
	"testing"

	"github.com/tsuna/gohbase"
)

func TestInitHbase(t *testing.T) {
	type args struct {
		zkQuorum string
		option   gohbase.Option
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			InitHbase(tt.args.zkQuorum, tt.args.option)
		})
	}
}
