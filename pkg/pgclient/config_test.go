package pgclient

import (
	"fmt"
	"testing"
	"time"
)

func TestConfig_GetConnectionStr(t *testing.T) {
	type fields struct {
		App                     string
		Host                    string
		Port                    int
		User                    string
		Password                string
		Database                string
		SslMode                 string
		DbConnectRetries        int
		DbConnectionTimeout     time.Duration
		AsyncAcks               bool
		ReportInterval          int
		LabelsCacheSize         uint64
		MetricsCacheSize        uint64
		SeriesCacheSize         uint64
		WriteConnectionsPerProc int
		MaxConnections          int
		UsesHA                  bool
		DbUri                   string
	}
	tests := []struct {
		name    string
		fields  fields
		want    string
		wantErr bool
		err     error
	}{
		{
			name: "Testcase with user provided db flag values and db-uri as default",
			fields: fields{
				App:                     DefaultApp,
				Host:                    "localhost",
				Port:                    5433,
				User:                    "postgres",
				Password:                "Timescale123",
				Database:                "timescale1",
				SslMode:                 "require",
				DbConnectRetries:        0,
				DbConnectionTimeout:     time.Minute * 2,
				AsyncAcks:               false,
				ReportInterval:          0,
				LabelsCacheSize:         0,
				MetricsCacheSize:        0,
				SeriesCacheSize:         0,
				WriteConnectionsPerProc: 1,
				MaxConnections:          0,
				UsesHA:                  false,
				DbUri:                   "",
			},
			want:    fmt.Sprintf("application_name=%s host=localhost port=5433 user=postgres dbname=timescale1 password='Timescale123' sslmode=require connect_timeout=120", DefaultApp),
			wantErr: false,
			err:     nil,
		},
		{
			name: "Testcase with both user provided db flags & db-uri combination 1",
			fields: fields{
				Host:                    "localhost",
				Port:                    5432,
				User:                    "postgres",
				Password:                "Timescale123",
				Database:                "timescale",
				SslMode:                 "require",
				DbConnectRetries:        0,
				AsyncAcks:               false,
				ReportInterval:          0,
				LabelsCacheSize:         0,
				MetricsCacheSize:        0,
				SeriesCacheSize:         0,
				WriteConnectionsPerProc: 1,
				MaxConnections:          0,
				UsesHA:                  false,
				DbUri:                   "postgres://postgres:password@localhost:5432/postgres?sslmode=allow",
			},
			want:    "",
			wantErr: true,
			err:     excessDBFlagsError,
		},
		{
			name: "Testcase with both user provided db flags & db-uri combination 2",
			fields: fields{
				Host:                    "localhost",
				Port:                    5433,
				User:                    "postgres",
				Password:                "Timescale123",
				Database:                "timescale",
				SslMode:                 "require",
				DbConnectRetries:        0,
				AsyncAcks:               false,
				ReportInterval:          0,
				LabelsCacheSize:         0,
				MetricsCacheSize:        0,
				SeriesCacheSize:         0,
				WriteConnectionsPerProc: 1,
				MaxConnections:          0,
				UsesHA:                  false,
				DbUri:                   "postgres://postgres",
			},
			want:    "",
			wantErr: true,
			err:     excessDBFlagsError,
		},
		{
			name: "Testcase with default db flags",
			fields: fields{
				App:                     DefaultApp,
				Host:                    "localhost",
				Port:                    5432,
				User:                    "postgres",
				Password:                "",
				Database:                "timescale",
				SslMode:                 "require",
				DbConnectRetries:        0,
				DbConnectionTimeout:     time.Hour,
				AsyncAcks:               false,
				ReportInterval:          0,
				LabelsCacheSize:         0,
				MetricsCacheSize:        0,
				SeriesCacheSize:         0,
				WriteConnectionsPerProc: 1,
				MaxConnections:          0,
				UsesHA:                  false,
				DbUri:                   "",
			},
			want:    fmt.Sprintf("application_name=%s host=localhost port=5432 user=postgres dbname=timescale password='' sslmode=require connect_timeout=3600", DefaultApp),
			wantErr: false,
			err:     nil,
		},
		{
			name: "Testcase with default db flags & user provided db-uri",
			fields: fields{
				App:                     DefaultApp,
				Host:                    "localhost",
				Port:                    5432,
				User:                    "postgres",
				Password:                "",
				Database:                "timescale",
				SslMode:                 "require",
				DbConnectRetries:        0,
				DbConnectionTimeout:     defaultConnectionTime,
				AsyncAcks:               false,
				ReportInterval:          0,
				LabelsCacheSize:         0,
				MetricsCacheSize:        0,
				SeriesCacheSize:         0,
				WriteConnectionsPerProc: 1,
				MaxConnections:          0,
				UsesHA:                  false,
				DbUri:                   "postgres://postgres:password@localhost:5432/postgres?sslmode=allow",
			},
			want:    "postgres://postgres:password@localhost:5432/postgres?sslmode=allow",
			wantErr: false,
			err:     nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{
				AppName:                 tt.fields.App,
				Host:                    tt.fields.Host,
				Port:                    tt.fields.Port,
				User:                    tt.fields.User,
				Password:                tt.fields.Password,
				Database:                tt.fields.Database,
				SslMode:                 tt.fields.SslMode,
				DbConnectRetries:        tt.fields.DbConnectRetries,
				DbConnectionTimeout:     tt.fields.DbConnectionTimeout,
				AsyncAcks:               tt.fields.AsyncAcks,
				ReportInterval:          tt.fields.ReportInterval,
				LabelsCacheSize:         tt.fields.LabelsCacheSize,
				MetricsCacheSize:        tt.fields.MetricsCacheSize,
				SeriesCacheSize:         tt.fields.SeriesCacheSize,
				WriteConnectionsPerProc: tt.fields.WriteConnectionsPerProc,
				MaxConnections:          tt.fields.MaxConnections,
				UsesHA:                  tt.fields.UsesHA,
				DbUri:                   tt.fields.DbUri,
			}
			got, err := cfg.GetConnectionStr()
			if (err != nil) != tt.wantErr || err != tt.err {
				t.Errorf("GetConnectionStr() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("GetConnectionStr() got = %v, want %v", got, tt.want)
			}
		})
	}
}
