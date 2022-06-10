// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package api

import (
	"fmt"
	"net/http"

	"github.com/timescale/promscale/pkg/log"
)

func Reload(reload func() error, webAdmin bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !webAdmin {
			err := fmt.Errorf("reload received but web admin is disabled. To enable, start Promscale with '-web.enable-admin-api' flag")
			log.Error("msg", err.Error())
			http.Error(w, fmt.Errorf("failed to reload: %w", err).Error(), http.StatusUnauthorized)
			return
		}
		if err := reload(); err != nil {
			log.Error("msg", "failed to reload", "err", err.Error())
			http.Error(w, fmt.Errorf("failed to reload: %w", err).Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Length", "0")
	}
}
