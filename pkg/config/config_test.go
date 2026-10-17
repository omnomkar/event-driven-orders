package config

import (
	"slices"
	"strings"
	"testing"
	"time"
)

// env builds a lookup function backed by a map, standing in for os.LookupEnv.
func env(vars map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		v, ok := vars[key]
		return v, ok
	}
}

func TestString(t *testing.T) {
	tests := []struct {
		name string
		vars map[string]string
		want string
	}{
		{"set", map[string]string{"ADDR": ":9001"}, ":9001"},
		{"unset uses default", nil, ":8080"},
		{"empty uses default", map[string]string{"ADDR": ""}, ":8080"},
		{"whitespace is trimmed", map[string]string{"ADDR": "  :9001 "}, ":9001"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := NewWithLookup(env(tt.vars))
			if got := l.String("ADDR", ":8080"); got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
			if err := l.Err(); err != nil {
				t.Errorf("Err() = %v, want nil", err)
			}
		})
	}
}

func TestRequired(t *testing.T) {
	tests := []struct {
		name    string
		vars    map[string]string
		want    string
		wantErr bool
	}{
		{"set", map[string]string{"DATABASE_URL": "postgres://db"}, "postgres://db", false},
		{"unset", nil, "", true},
		{"empty", map[string]string{"DATABASE_URL": " "}, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := NewWithLookup(env(tt.vars))
			got := l.Required("DATABASE_URL")
			if got != tt.want {
				t.Errorf("Required() = %q, want %q", got, tt.want)
			}
			if (l.Err() != nil) != tt.wantErr {
				t.Errorf("Err() = %v, wantErr %v", l.Err(), tt.wantErr)
			}
		})
	}
}

func TestInt(t *testing.T) {
	tests := []struct {
		name    string
		vars    map[string]string
		want    int
		wantErr bool
	}{
		{"set", map[string]string{"N": "42"}, 42, false},
		{"unset uses default", nil, 7, false},
		{"invalid", map[string]string{"N": "forty"}, 7, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := NewWithLookup(env(tt.vars))
			if got := l.Int("N", 7); got != tt.want {
				t.Errorf("Int() = %d, want %d", got, tt.want)
			}
			if (l.Err() != nil) != tt.wantErr {
				t.Errorf("Err() = %v, wantErr %v", l.Err(), tt.wantErr)
			}
		})
	}
}

func TestBool(t *testing.T) {
	tests := []struct {
		name    string
		vars    map[string]string
		def     bool
		want    bool
		wantErr bool
	}{
		{"true", map[string]string{"B": "true"}, false, true, false},
		{"false overrides default", map[string]string{"B": "false"}, true, false, false},
		{"numeric", map[string]string{"B": "0"}, true, false, false},
		{"unset uses default", nil, true, true, false},
		{"invalid", map[string]string{"B": "yes please"}, true, true, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := NewWithLookup(env(tt.vars))
			if got := l.Bool("B", tt.def); got != tt.want {
				t.Errorf("Bool() = %v, want %v", got, tt.want)
			}
			if (l.Err() != nil) != tt.wantErr {
				t.Errorf("Err() = %v, wantErr %v", l.Err(), tt.wantErr)
			}
		})
	}
}

func TestDuration(t *testing.T) {
	tests := []struct {
		name    string
		vars    map[string]string
		want    time.Duration
		wantErr bool
	}{
		{"set", map[string]string{"T": "3s"}, 3 * time.Second, false},
		{"unset uses default", nil, 10 * time.Second, false},
		{"invalid", map[string]string{"T": "soon"}, 10 * time.Second, true},
		{"negative", map[string]string{"T": "-1s"}, 10 * time.Second, true},
		{"zero", map[string]string{"T": "0s"}, 10 * time.Second, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := NewWithLookup(env(tt.vars))
			if got := l.Duration("T", 10*time.Second); got != tt.want {
				t.Errorf("Duration() = %v, want %v", got, tt.want)
			}
			if (l.Err() != nil) != tt.wantErr {
				t.Errorf("Err() = %v, wantErr %v", l.Err(), tt.wantErr)
			}
		})
	}
}

func TestRequiredList(t *testing.T) {
	tests := []struct {
		name    string
		vars    map[string]string
		want    []string
		wantErr bool
	}{
		{"single", map[string]string{"B": "kafka:9092"}, []string{"kafka:9092"}, false},
		{"several with blanks", map[string]string{"B": "a:1, ,b:2,"}, []string{"a:1", "b:2"}, false},
		{"unset", nil, nil, true},
		{"only commas", map[string]string{"B": ",,"}, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := NewWithLookup(env(tt.vars))
			if got := l.RequiredList("B"); !slices.Equal(got, tt.want) {
				t.Errorf("RequiredList() = %v, want %v", got, tt.want)
			}
			if (l.Err() != nil) != tt.wantErr {
				t.Errorf("Err() = %v, wantErr %v", l.Err(), tt.wantErr)
			}
		})
	}
}

// TestErrReportsEveryProblem checks that all problems surface together, not
// just the first one.
func TestErrReportsEveryProblem(t *testing.T) {
	l := NewWithLookup(env(map[string]string{"N": "x"}))
	l.Required("DATABASE_URL")
	l.Required("KAFKA_BROKERS")
	l.Int("N", 0)

	err := l.Err()
	if err == nil {
		t.Fatal("Err() = nil, want an error")
	}
	for _, want := range []string{"DATABASE_URL", "KAFKA_BROKERS", "N:"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("Err() = %q, missing %q", err, want)
		}
	}
}
