package config

import "testing"

func TestConfig_AsyncQueueAnalyzerEnabled(t *testing.T) {
	tests := []struct {
		name     string
		analyzer []AnalyzerScoreConfig
		want     bool
	}{
		{
			name:     "absent — no saturation config entries",
			analyzer: nil,
			want:     false,
		},
		{
			name: "absent — other analyzers present, async_queue missing",
			analyzer: []AnalyzerScoreConfig{
				{Name: "saturation"},
			},
			want: false,
		},
		{
			name: "enabled — present with Enabled nil (defaults true)",
			analyzer: []AnalyzerScoreConfig{
				{Name: asyncQueueAnalyzerName},
			},
			want: true,
		},
		{
			name: "disabled — explicit Enabled:false",
			analyzer: []AnalyzerScoreConfig{
				{Name: asyncQueueAnalyzerName, Enabled: boolPtr(false)},
			},
			want: false,
		},
		{
			name: "enabled — matched via Type override, Name differs",
			analyzer: []AnalyzerScoreConfig{
				{Name: "my-async", Type: asyncQueueAnalyzerName},
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := NewTestConfig()
			cfg.UpdateSaturationConfig(map[string]SaturationScalingConfig{
				"default": {Analyzers: tt.analyzer},
			})
			if got := cfg.AsyncQueueAnalyzerEnabled(); got != tt.want {
				t.Errorf("AsyncQueueAnalyzerEnabled() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConfig_AsyncBacklogQuery(t *testing.T) {
	tests := []struct {
		name     string
		analyzer []AnalyzerScoreConfig
		want     string
	}{
		{
			name:     "empty when no async_queue entry",
			analyzer: []AnalyzerScoreConfig{{Name: "saturation"}},
			want:     "",
		},
		{
			name: "empty when async_queue entry sets no backlogQuery",
			analyzer: []AnalyzerScoreConfig{
				{Name: asyncQueueAnalyzerName},
			},
			want: "",
		},
		{
			name: "returns the operator-supplied backlogQuery",
			analyzer: []AnalyzerScoreConfig{
				{Name: asyncQueueAnalyzerName, Parameters: map[string]any{
					asyncBacklogQueryParam: `sum(my_backlog)`,
				}},
			},
			want: `sum(my_backlog)`,
		},
		{
			name: "ignores backlogQuery on a disabled entry",
			analyzer: []AnalyzerScoreConfig{
				{Name: asyncQueueAnalyzerName, Enabled: boolPtr(false), Parameters: map[string]any{
					asyncBacklogQueryParam: `sum(my_backlog)`,
				}},
			},
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := NewTestConfig()
			cfg.UpdateSaturationConfig(map[string]SaturationScalingConfig{
				"default": {Analyzers: tt.analyzer},
			})
			if got := cfg.AsyncBacklogQuery(); got != tt.want {
				t.Errorf("AsyncBacklogQuery() = %q, want %q", got, tt.want)
			}
		})
	}
}
