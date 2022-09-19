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
		App                 string
		Host                string
		Port                int
		User                string
		Password            string
		Database            string
		SslMode             string
		DbConnectionTimeout time.Duration
		AsyncAcks           bool
		LabelsCacheSize     uint64
		MetricsCacheSize    uint64
		SeriesCacheSize     uint64
		WriteConnections    int
		MaxConnections      int
		UsesHA              bool
		DbUri               string
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
				App:                 DefaultApp,
				Host:                "localhost",
				Port:                5433,
				User:                "postgres",
				Password:            "Timescale123",
				Database:            "timescale1",
				SslMode:             "require",
				DbConnectionTimeout: time.Minute * 2,
				AsyncAcks:           false,
				LabelsCacheSize:     0,
				MetricsCacheSize:    0,
				SeriesCacheSize:     0,
				WriteConnections:    0,
				MaxConnections:      0,
				UsesHA:              false,
				DbUri:               "",
			},
			want:    fmt.Sprintf("postgresql://postgres:Timescale123@localhost:5433/timescale1?application_name=%s&connect_timeout=120&sslmode=require", url.QueryEscape(DefaultApp)),
			wantErr: false,
			err:     nil,
		},
		{
			name: "Testcase with both user provided db flags & db-uri combination 1",
			fields: fields{
				Host:             "localhost",
				Port:             5432,
				User:             "postgres",
				Password:         "Timescale123",
				Database:         "timescale",
				SslMode:          "require",
				AsyncAcks:        false,
				LabelsCacheSize:  0,
				MetricsCacheSize: 0,
				SeriesCacheSize:  0,
				WriteConnections: 0,
				MaxConnections:   0,
				UsesHA:           false,
				DbUri:            "postgres://postgres:password@localhost:5432/postgres?sslmode=allow",
			},
			want:    "",
			wantErr: true,
			err:     excessDBFlagsError,
		},
		{
			name: "Testcase with both user provided db flags & db-uri combination 2",
			fields: fields{
				Host:             "localhost",
				Port:             5433,
				User:             "postgres",
				Password:         "Timescale123",
				Database:         "timescale",
				SslMode:          "require",
				AsyncAcks:        false,
				LabelsCacheSize:  0,
				MetricsCacheSize: 0,
				SeriesCacheSize:  0,
				WriteConnections: 0,
				MaxConnections:   0,
				UsesHA:           false,
				DbUri:            "postgres://postgres",
			},
			want:    "",
			wantErr: true,
			err:     excessDBFlagsError,
		},
		{
			name: "Testcase with default db flags",
			fields: fields{
				App:                 DefaultApp,
				Host:                "localhost",
				Port:                5432,
				User:                "postgres",
				Password:            "",
				Database:            "timescale",
				SslMode:             "require",
				DbConnectionTimeout: time.Hour,
				AsyncAcks:           false,
				LabelsCacheSize:     0,
				MetricsCacheSize:    0,
				SeriesCacheSize:     0,
				WriteConnections:    0,
				MaxConnections:      0,
				UsesHA:              false,
				DbUri:               "",
			},
			want:    fmt.Sprintf("postgresql://postgres:@localhost:5432/timescale?application_name=%s&connect_timeout=3600&sslmode=require", url.QueryEscape(DefaultApp)),
			wantErr: false,
			err:     nil,
		},
		{
			name: "Testcase with default db flags & user provided db-uri",
			fields: fields{
				App:                 DefaultApp,
				Host:                "localhost",
				Port:                5432,
				User:                "postgres",
				Password:            "",
				Database:            "timescale",
				SslMode:             "require",
				DbConnectionTimeout: defaultConnectionTime,
				AsyncAcks:           false,
				LabelsCacheSize:     0,
				MetricsCacheSize:    0,
				SeriesCacheSize:     0,
				WriteConnections:    0,
				MaxConnections:      0,
				UsesHA:              false,
				DbUri:               "postgres://postgres:password@localhost:5432/postgres?sslmode=allow",
			},
			want:    "postgres://postgres:password@localhost:5432/postgres?sslmode=allow",
			wantErr: false,
			err:     nil,
		},
		{
			name: "Testcase with db-uri that has no question mark in it",
			fields: fields{
				App:                 DefaultApp,
				Host:                "localhost",
				Port:                5432,
				User:                "postgres",
				Password:            "",
				Database:            "timescale",
				SslMode:             "require",
				DbConnectionTimeout: defaultConnectionTime,
				AsyncAcks:           false,
				LabelsCacheSize:     0,
				MetricsCacheSize:    0,
				SeriesCacheSize:     0,
				WriteConnections:    0,
				MaxConnections:      0,
				UsesHA:              false,
				DbUri:               "postgres://postgres:password@localhost:5432/postgres",
			},
			want:    "postgres://postgres:password@localhost:5432/postgres",
			wantErr: false,
			err:     nil,
		},
		{
			name: "Testcase with db-uri that requires password to be URL encoded",
			fields: fields{
				App:                 DefaultApp,
				Host:                "localhost",
				Port:                5432,
				User:                "postgres",
				Password:            ":/|thi$ i$ a p@$$w0rd #!***",
				Database:            "timescale",
				SslMode:             "require",
				DbConnectionTimeout: defaultConnectionTime,
				AsyncAcks:           false,
				LabelsCacheSize:     0,
				MetricsCacheSize:    0,
				SeriesCacheSize:     0,
				WriteConnections:    0,
				MaxConnections:      0,
				UsesHA:              false,
				DbUri:               "",
			},
			want:    fmt.Sprintf("postgresql://postgres:%s@localhost:5432/timescale?application_name=%s&connect_timeout=60&sslmode=require", "%3A%2F%7Cthi$%20i$%20a%20p%40$$w0rd%20%23%21%2A%2A%2A", url.QueryEscape(DefaultApp)),
			wantErr: false,
			err:     nil,
		},
		{
			name: "Testcase with db-uri that requires user to be URL encoded",
			fields: fields{
				App:                 DefaultApp,
				Host:                "localhost",
				Port:                5432,
				User:                "{- i @m & 1337 h@x0r -}",
				Password:            "password",
				Database:            "timescale",
				SslMode:             "require",
				DbConnectionTimeout: defaultConnectionTime,
				AsyncAcks:           false,
				LabelsCacheSize:     0,
				MetricsCacheSize:    0,
				SeriesCacheSize:     0,
				WriteConnections:    0,
				MaxConnections:      0,
				UsesHA:              false,
				DbUri:               "",
			},
			want:    fmt.Sprintf("postgresql://%s:password@localhost:5432/timescale?application_name=%s&connect_timeout=60&sslmode=require", "%7B-%20i%20%40m%20&%201337%20h%40x0r%20-%7D", url.QueryEscape(DefaultApp)),
			wantErr: false,
			err:     nil,
		},
		{
			name: "Testcase with db-uri that requires database to be URL encoded",
			fields: fields{
				App:                 DefaultApp,
				Host:                "localhost",
				Port:                5432,
				User:                "postgres",
				Password:            "password",
				Database:            "%~~ $uP3r Funky d@7@b@$$ ~~%",
				SslMode:             "require",
				DbConnectionTimeout: defaultConnectionTime,
				AsyncAcks:           false,
				LabelsCacheSize:     0,
				MetricsCacheSize:    0,
				SeriesCacheSize:     0,
				WriteConnections:    0,
				MaxConnections:      0,
				UsesHA:              false,
				DbUri:               "",
			},
			want:    fmt.Sprintf("postgresql://postgres:password@localhost:5432/%s?application_name=%s&connect_timeout=60&sslmode=require", "%25~~%20$uP3r%20Funky%20d@7@b@$$%20~~%25", url.QueryEscape(DefaultApp)),
			wantErr: false,
			err:     nil,
		},
		{
			name: "Testcase with db-uri that requires application to be URL encoded",
			fields: fields{
				App:                 "!<-@ppL1c@t10N->!",
				Host:                "localhost",
				Port:                5432,
				User:                "postgres",
				Password:            "password",
				Database:            "timescale",
				SslMode:             "require",
				DbConnectionTimeout: defaultConnectionTime,
				AsyncAcks:           false,
				LabelsCacheSize:     0,
				MetricsCacheSize:    0,
				SeriesCacheSize:     0,
				WriteConnections:    0,
				MaxConnections:      0,
				UsesHA:              false,
				DbUri:               "",
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
				AppName:             tt.fields.App,
				Host:                tt.fields.Host,
				Port:                tt.fields.Port,
				User:                tt.fields.User,
				Password:            tt.fields.Password,
				Database:            tt.fields.Database,
				SslMode:             tt.fields.SslMode,
				DbConnectionTimeout: tt.fields.DbConnectionTimeout,
				WriteConnections:    tt.fields.WriteConnections,
				MaxConnections:      tt.fields.MaxConnections,
				UsesHA:              tt.fields.UsesHA,
				DbUri:               tt.fields.DbUri,
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

func TestConfig_GetPoolSizes(t *testing.T) {
	type config struct {
		WriterPoolSize int
		ReaderPoolSize int
		MaintPoolSize  int
		MaxConnections int
		UsesHA         bool
	}
	type args struct {
		readOnly bool
		maxConns int
	}
	tests := []struct {
		name               string
		config             config
		args               args
		wantReaderPoolSize int
		wantWriterPoolSize int
		wantMaintPoolSize  int
		wantErr            bool
	}{
		{
			name: "read only defaults max 100",
			config: config{
				WriterPoolSize: -1,
				ReaderPoolSize: -1,
				MaintPoolSize:  -1,
				MaxConnections: -1,
				UsesHA:         false,
			},
			args: args{
				readOnly: true,
				maxConns: 100,
			},
			wantReaderPoolSize: 50,
			wantWriterPoolSize: 0,
			wantMaintPoolSize:  5,
			wantErr:            false,
		},
		{
			name: "read only defaults max 75",
			config: config{
				WriterPoolSize: -1,
				ReaderPoolSize: -1,
				MaintPoolSize:  -1,
				MaxConnections: -1,
				UsesHA:         false,
			},
			args: args{
				readOnly: true,
				maxConns: 75,
			},
			wantReaderPoolSize: 37,
			wantWriterPoolSize: 0,
			wantMaintPoolSize:  5,
			wantErr:            false,
		},
		{
			name: "read only defaults HA max 100",
			config: config{
				WriterPoolSize: -1,
				ReaderPoolSize: -1,
				MaintPoolSize:  -1,
				MaxConnections: -1,
				UsesHA:         true,
			},
			args: args{
				readOnly: true,
				maxConns: 100,
			},
			wantReaderPoolSize: 25,
			wantWriterPoolSize: 0,
			wantMaintPoolSize:  5,
			wantErr:            false,
		},
		{
			name: "small max conn",
			config: config{
				WriterPoolSize: -1,
				ReaderPoolSize: -1,
				MaintPoolSize:  -1,
				MaxConnections: -1,
				UsesHA:         true,
			},
			args: args{
				readOnly: false,
				maxConns: 10,
			},
			wantReaderPoolSize: 0,
			wantWriterPoolSize: 0,
			wantMaintPoolSize:  0,
			wantErr:            true,
		},
		{
			name: "read only too high",
			config: config{
				WriterPoolSize: -1,
				ReaderPoolSize: 110,
				MaintPoolSize:  -1,
				MaxConnections: -1,
				UsesHA:         false,
			},
			args: args{
				readOnly: true,
				maxConns: 100,
			},
			wantReaderPoolSize: 0,
			wantWriterPoolSize: 0,
			wantMaintPoolSize:  0,
			wantErr:            true,
		},
		{
			name: "read only HA too high",
			config: config{
				WriterPoolSize: -1,
				ReaderPoolSize: 220,
				MaintPoolSize:  -1,
				MaxConnections: -1,
				UsesHA:         true,
			},
			args: args{
				readOnly: true,
				maxConns: 100,
			},
			wantReaderPoolSize: 0,
			wantWriterPoolSize: 0,
			wantMaintPoolSize:  0,
			wantErr:            true,
		},
		{
			name: "reader too high",
			config: config{
				WriterPoolSize: -1,
				ReaderPoolSize: 101,
				MaintPoolSize:  -1,
				MaxConnections: -1,
				UsesHA:         false,
			},
			args: args{
				readOnly: false,
				maxConns: 100,
			},
			wantReaderPoolSize: 0,
			wantWriterPoolSize: 0,
			wantMaintPoolSize:  0,
			wantErr:            true,
		},
		{
			name: "writer too high",
			config: config{
				WriterPoolSize: 101,
				ReaderPoolSize: -1,
				MaintPoolSize:  -1,
				MaxConnections: -1,
				UsesHA:         false,
			},
			args: args{
				readOnly: false,
				maxConns: 100,
			},
			wantReaderPoolSize: 0,
			wantWriterPoolSize: 0,
			wantMaintPoolSize:  0,
			wantErr:            true,
		},
		{
			name: "maint too high",
			config: config{
				WriterPoolSize: -1,
				ReaderPoolSize: -1,
				MaintPoolSize:  101,
				MaxConnections: -1,
				UsesHA:         true,
			},
			args: args{
				readOnly: true,
				maxConns: 100,
			},
			wantReaderPoolSize: 0,
			wantWriterPoolSize: 0,
			wantMaintPoolSize:  0,
			wantErr:            true,
		},
		{
			name: "defaults",
			config: config{
				WriterPoolSize: -1,
				ReaderPoolSize: -1,
				MaintPoolSize:  -1,
				MaxConnections: -1,
				UsesHA:         false,
			},
			args: args{
				readOnly: false,
				maxConns: 100,
			},
			wantReaderPoolSize: 25,
			wantWriterPoolSize: 50,
			wantMaintPoolSize:  5,
			wantErr:            false,
		},
		{
			name: "defaults HA",
			config: config{
				WriterPoolSize: -1,
				ReaderPoolSize: -1,
				MaintPoolSize:  -1,
				MaxConnections: -1,
				UsesHA:         true,
			},
			args: args{
				readOnly: false,
				maxConns: 100,
			},
			wantReaderPoolSize: 15,
			wantWriterPoolSize: 25,
			wantMaintPoolSize:  5,
			wantErr:            false,
		},
		{
			name: "exact 1",
			config: config{
				WriterPoolSize: 50,
				ReaderPoolSize: 25,
				MaintPoolSize:  5,
				MaxConnections: -1,
				UsesHA:         false,
			},
			args: args{
				readOnly: false,
				maxConns: 100,
			},
			wantReaderPoolSize: 25,
			wantWriterPoolSize: 50,
			wantMaintPoolSize:  5,
			wantErr:            false,
		},
		{
			name: "exacts greater than default db.connections-max",
			config: config{
				WriterPoolSize: 51,
				ReaderPoolSize: 25,
				MaintPoolSize:  5,
				MaxConnections: -1,
				UsesHA:         false,
			},
			args: args{
				readOnly: false,
				maxConns: 100,
			},
			wantReaderPoolSize: 0,
			wantWriterPoolSize: 0,
			wantMaintPoolSize:  0,
			wantErr:            true,
		},
		{
			name: "exacts greater than specified db.connections-max",
			config: config{
				WriterPoolSize: 50,
				ReaderPoolSize: 25,
				MaintPoolSize:  5,
				MaxConnections: 79,
				UsesHA:         false,
			},
			args: args{
				readOnly: false,
				maxConns: 100,
			},
			wantReaderPoolSize: 0,
			wantWriterPoolSize: 0,
			wantMaintPoolSize:  0,
			wantErr:            true,
		},
		{
			name: "exact HA",
			config: config{
				WriterPoolSize: 50,
				ReaderPoolSize: 25,
				MaintPoolSize:  5,
				MaxConnections: -1,
				UsesHA:         true,
			},
			args: args{
				readOnly: false,
				maxConns: 100,
			},
			wantReaderPoolSize: 25,
			wantWriterPoolSize: 50,
			wantMaintPoolSize:  5,
			wantErr:            false,
		},
		{
			name: "exact = max_connections",
			config: config{
				WriterPoolSize: 50,
				ReaderPoolSize: 45,
				MaintPoolSize:  5,
				MaxConnections: 100,
				UsesHA:         false,
			},
			args: args{
				readOnly: false,
				maxConns: 100,
			},
			wantReaderPoolSize: 45,
			wantWriterPoolSize: 50,
			wantMaintPoolSize:  5,
			wantErr:            false,
		},
		{
			name: "exact = max_connections 2",
			config: config{
				WriterPoolSize: 50,
				ReaderPoolSize: 45,
				MaintPoolSize:  5,
				MaxConnections: 1000,
				UsesHA:         false,
			},
			args: args{
				readOnly: false,
				maxConns: 100,
			},
			wantReaderPoolSize: 45,
			wantWriterPoolSize: 50,
			wantMaintPoolSize:  5,
			wantErr:            false,
		},
		{
			name: "exact > cfg max",
			config: config{
				WriterPoolSize: 50,
				ReaderPoolSize: 45,
				MaintPoolSize:  15,
				MaxConnections: -1,
				UsesHA:         false,
			},
			args: args{
				readOnly: false,
				maxConns: 100,
			},
			wantReaderPoolSize: 0,
			wantWriterPoolSize: 0,
			wantMaintPoolSize:  0,
			wantErr:            true,
		},
		{
			name: "exact > db max",
			config: config{
				WriterPoolSize: 50,
				ReaderPoolSize: 45,
				MaintPoolSize:  15,
				MaxConnections: 1000,
				UsesHA:         false,
			},
			args: args{
				readOnly: false,
				maxConns: 100,
			},
			wantReaderPoolSize: 0,
			wantWriterPoolSize: 0,
			wantMaintPoolSize:  0,
			wantErr:            true,
		},
		{
			name: "explicit reader",
			config: config{
				WriterPoolSize: -1,
				ReaderPoolSize: 15,
				MaintPoolSize:  -1,
				MaxConnections: -1,
				UsesHA:         false,
			},
			args: args{
				readOnly: false,
				maxConns: 100,
			},
			wantReaderPoolSize: 15,
			wantWriterPoolSize: 50,
			wantMaintPoolSize:  5,
			wantErr:            false,
		},
		{
			name: "explicit reader 2",
			config: config{
				WriterPoolSize: -1,
				ReaderPoolSize: 45,
				MaintPoolSize:  -1,
				MaxConnections: -1,
				UsesHA:         false,
			},
			args: args{
				readOnly: false,
				maxConns: 100,
			},
			wantReaderPoolSize: 0,
			wantWriterPoolSize: 0,
			wantMaintPoolSize:  0,
			wantErr:            true,
		},
		{
			name: "explicit writer",
			config: config{
				WriterPoolSize: 50,
				ReaderPoolSize: -1,
				MaintPoolSize:  -1,
				MaxConnections: -1,
				UsesHA:         false,
			},
			args: args{
				readOnly: false,
				maxConns: 100,
			},
			wantReaderPoolSize: 25,
			wantWriterPoolSize: 50,
			wantMaintPoolSize:  5,
			wantErr:            false,
		},
		{
			name: "explicit writer 2",
			config: config{
				WriterPoolSize: 60,
				ReaderPoolSize: -1,
				MaintPoolSize:  -1,
				MaxConnections: -1,
				UsesHA:         false,
			},
			args: args{
				readOnly: false,
				maxConns: 100,
			},
			wantReaderPoolSize: 0,
			wantWriterPoolSize: 0,
			wantMaintPoolSize:  0,
			wantErr:            true,
		},
		{
			name: "explicit writer & maint",
			config: config{
				WriterPoolSize: 60,
				ReaderPoolSize: -1,
				MaintPoolSize:  10,
				MaxConnections: -1,
				UsesHA:         false,
			},
			args: args{
				readOnly: false,
				maxConns: 100,
			},
			wantReaderPoolSize: 0,
			wantWriterPoolSize: 0,
			wantMaintPoolSize:  0,
			wantErr:            true,
		},
		{
			name: "explicit writer & maint 2",
			config: config{
				WriterPoolSize: 30,
				ReaderPoolSize: -1,
				MaintPoolSize:  10,
				MaxConnections: -1,
				UsesHA:         false,
			},
			args: args{
				readOnly: false,
				maxConns: 100,
			},
			wantReaderPoolSize: 30,
			wantWriterPoolSize: 30,
			wantMaintPoolSize:  10,
			wantErr:            false,
		},
		{
			name: "explicit reader & maint",
			config: config{
				WriterPoolSize: -1,
				ReaderPoolSize: 40,
				MaintPoolSize:  10,
				MaxConnections: -1,
				UsesHA:         false,
			},
			args: args{
				readOnly: false,
				maxConns: 100,
			},
			wantReaderPoolSize: 0,
			wantWriterPoolSize: 0,
			wantMaintPoolSize:  0,
			wantErr:            true,
		},
		{
			name: "explicit reader & maint 2",
			config: config{
				WriterPoolSize: -1,
				ReaderPoolSize: 20,
				MaintPoolSize:  10,
				MaxConnections: -1,
				UsesHA:         false,
			},
			args: args{
				readOnly: false,
				maxConns: 100,
			},
			wantReaderPoolSize: 20,
			wantWriterPoolSize: 50,
			wantMaintPoolSize:  10,
			wantErr:            false,
		},
		{
			name: "maint = 1",
			config: config{
				WriterPoolSize: -1,
				ReaderPoolSize: -1,
				MaintPoolSize:  1,
				MaxConnections: -1,
				UsesHA:         false,
			},
			args: args{
				readOnly: false,
				maxConns: 100,
			},
			wantReaderPoolSize: 0,
			wantWriterPoolSize: 0,
			wantMaintPoolSize:  0,
			wantErr:            true,
		},
		{
			name: "reader = 1",
			config: config{
				WriterPoolSize: -1,
				ReaderPoolSize: 1,
				MaintPoolSize:  -1,
				MaxConnections: -1,
				UsesHA:         false,
			},
			args: args{
				readOnly: false,
				maxConns: 100,
			},
			wantReaderPoolSize: 0,
			wantWriterPoolSize: 0,
			wantMaintPoolSize:  0,
			wantErr:            true,
		},
		{
			name: "writer = 1",
			config: config{
				WriterPoolSize: 1,
				ReaderPoolSize: -1,
				MaintPoolSize:  -1,
				MaxConnections: -1,
				UsesHA:         false,
			},
			args: args{
				readOnly: false,
				maxConns: 100,
			},
			wantReaderPoolSize: 0,
			wantWriterPoolSize: 0,
			wantMaintPoolSize:  0,
			wantErr:            true,
		},
		{
			name: "ha writer = 2",
			config: config{
				WriterPoolSize: 2,
				ReaderPoolSize: -1,
				MaintPoolSize:  -1,
				MaxConnections: -1,
				UsesHA:         true,
			},
			args: args{
				readOnly: false,
				maxConns: 100,
			},
			wantReaderPoolSize: 15,
			wantWriterPoolSize: 2,
			wantMaintPoolSize:  5,
			wantErr:            false,
		},
		{
			name: "ha reader = 2",
			config: config{
				WriterPoolSize: -1,
				ReaderPoolSize: 2,
				MaintPoolSize:  -1,
				MaxConnections: -1,
				UsesHA:         true,
			},
			args: args{
				readOnly: false,
				maxConns: 100,
			},
			wantReaderPoolSize: 2,
			wantWriterPoolSize: 25,
			wantMaintPoolSize:  5,
			wantErr:            false,
		},
		{
			name: "maint = 2",
			config: config{
				WriterPoolSize: -1,
				ReaderPoolSize: -1,
				MaintPoolSize:  2,
				MaxConnections: -1,
				UsesHA:         false,
			},
			args: args{
				readOnly: false,
				maxConns: 100,
			},
			wantReaderPoolSize: 28,
			wantWriterPoolSize: 50,
			wantMaintPoolSize:  2,
			wantErr:            false,
		},
		{
			name: "ha maint = 2",
			config: config{
				WriterPoolSize: -1,
				ReaderPoolSize: -1,
				MaintPoolSize:  2,
				MaxConnections: -1,
				UsesHA:         true,
			},
			args: args{
				readOnly: false,
				maxConns: 100,
			},
			wantReaderPoolSize: 15,
			wantWriterPoolSize: 25,
			wantMaintPoolSize:  2,
			wantErr:            false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{
				WriterPoolSize: tt.config.WriterPoolSize,
				ReaderPoolSize: tt.config.ReaderPoolSize,
				MaintPoolSize:  tt.config.MaintPoolSize,
				UsesHA:         tt.config.UsesHA,
				MaxConnections: tt.config.MaxConnections,
			}
			gotReaderPoolSize, gotWriterPoolSize, gotMaintPoolSize, err := cfg.GetPoolSizes(tt.args.readOnly, tt.args.maxConns)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetPoolSizes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotReaderPoolSize != tt.wantReaderPoolSize {
				t.Errorf("GetPoolSizes() gotReaderPoolSize = %v, want %v", gotReaderPoolSize, tt.wantReaderPoolSize)
			}
			if gotWriterPoolSize != tt.wantWriterPoolSize {
				t.Errorf("GetPoolSizes() gotWriterPoolSize = %v, want %v", gotWriterPoolSize, tt.wantWriterPoolSize)
			}
			if gotMaintPoolSize != tt.wantMaintPoolSize {
				t.Errorf("GetPoolSizes() gotMaintPoolSize = %v, want %v", gotMaintPoolSize, tt.wantMaintPoolSize)
			}
		})
	}
}
