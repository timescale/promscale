// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package pgclient

import (
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/timescale/promscale/pkg/pgmodel/cache"
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
			want:    fmt.Sprintf("postgresql://postgres:Timescale123@localhost:5433/timescale1?application_name=%s&connect_timeout=120&sslmode=require", url.QueryEscape(DefaultApp)),
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
			want:    fmt.Sprintf("postgresql://postgres:@localhost:5432/timescale?application_name=%s&connect_timeout=3600&sslmode=require", url.QueryEscape(DefaultApp)),
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
		{
			name: "Testcase with db-uri that has no question mark in it",
			fields: fields{
				App:                     DefaultApp,
				Host:                    "localhost",
				Port:                    5432,
				User:                    "postgres",
				Password:                "",
				Database:                "timescale",
				SslMode:                 "require",
				DbConnectionTimeout:     defaultConnectionTime,
				AsyncAcks:               false,
				ReportInterval:          0,
				LabelsCacheSize:         0,
				MetricsCacheSize:        0,
				SeriesCacheSize:         0,
				WriteConnectionsPerProc: 1,
				MaxConnections:          0,
				UsesHA:                  false,
				DbUri:                   "postgres://postgres:password@localhost:5432/postgres",
			},
			want:    "postgres://postgres:password@localhost:5432/postgres",
			wantErr: false,
			err:     nil,
		},
		{
			name: "Testcase with db-uri that requires password to be URL encoded",
			fields: fields{
				App:                     DefaultApp,
				Host:                    "localhost",
				Port:                    5432,
				User:                    "postgres",
				Password:                ":/|thi$ i$ a p@$$w0rd #!***",
				Database:                "timescale",
				SslMode:                 "require",
				DbConnectionTimeout:     defaultConnectionTime,
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
			want:    fmt.Sprintf("postgresql://postgres:%s@localhost:5432/timescale?application_name=%s&connect_timeout=60&sslmode=require", "%3A%2F%7Cthi$%20i$%20a%20p%40$$w0rd%20%23%21%2A%2A%2A", url.QueryEscape(DefaultApp)),
			wantErr: false,
			err:     nil,
		},
		{
			name: "Testcase with db-uri that requires user to be URL encoded",
			fields: fields{
				App:                     DefaultApp,
				Host:                    "localhost",
				Port:                    5432,
				User:                    "{- i @m & 1337 h@x0r -}",
				Password:                "password",
				Database:                "timescale",
				SslMode:                 "require",
				DbConnectionTimeout:     defaultConnectionTime,
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
			want:    fmt.Sprintf("postgresql://%s:password@localhost:5432/timescale?application_name=%s&connect_timeout=60&sslmode=require", "%7B-%20i%20%40m%20&%201337%20h%40x0r%20-%7D", url.QueryEscape(DefaultApp)),
			wantErr: false,
			err:     nil,
		},
		{
			name: "Testcase with db-uri that requires database to be URL encoded",
			fields: fields{
				App:                     DefaultApp,
				Host:                    "localhost",
				Port:                    5432,
				User:                    "postgres",
				Password:                "password",
				Database:                "%~~ $uP3r Funky d@7@b@$$ ~~%",
				SslMode:                 "require",
				DbConnectionTimeout:     defaultConnectionTime,
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
			want:    fmt.Sprintf("postgresql://postgres:password@localhost:5432/%s?application_name=%s&connect_timeout=60&sslmode=require", "%25~~%20$uP3r%20Funky%20d@7@b@$$%20~~%25", url.QueryEscape(DefaultApp)),
			wantErr: false,
			err:     nil,
		},
		{
			name: "Testcase with db-uri that requires application to be URL encoded",
			fields: fields{
				App:                     "!<-@ppL1c@t10N->!",
				Host:                    "localhost",
				Port:                    5432,
				User:                    "postgres",
				Password:                "password",
				Database:                "timescale",
				SslMode:                 "require",
				DbConnectionTimeout:     defaultConnectionTime,
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
			want:    fmt.Sprintf("postgresql://postgres:password@localhost:5432/timescale?application_name=%s&connect_timeout=60&sslmode=require", "%21%3C-%40ppL1c%40t10N-%3E%21"),
			wantErr: false,
			err:     nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{
				CacheConfig: cache.Config{
					LabelsCacheSize:        tt.fields.LabelsCacheSize,
					MetricsCacheSize:       tt.fields.MetricsCacheSize,
					SeriesCacheInitialSize: tt.fields.SeriesCacheSize,
				},
				AppName:                 tt.fields.App,
				Host:                    tt.fields.Host,
				Port:                    tt.fields.Port,
				User:                    tt.fields.User,
				Password:                tt.fields.Password,
				Database:                tt.fields.Database,
				SslMode:                 tt.fields.SslMode,
				DbConnectionTimeout:     tt.fields.DbConnectionTimeout,
				WriteConnectionsPerProc: tt.fields.WriteConnectionsPerProc,
				MaxConnections:          tt.fields.MaxConnections,
				UsesHA:                  tt.fields.UsesHA,
				DbUri:                   tt.fields.DbUri,
			}
			err := cfg.validateConnectionSettings()
			if (err != nil) != tt.wantErr || err != tt.err {
				t.Errorf("validateConnectionSettings() error = %v, wantErr %v", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}
			got := cfg.GetConnectionStr()
			if got != tt.want {
				t.Errorf("GetConnectionStr() \ngot  %v \nwant %v", got, tt.want)
			}
		})
	}
}
