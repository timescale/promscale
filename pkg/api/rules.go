// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package api

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/NYTimes/gziphandler"

	"github.com/prometheus/prometheus/model/labels"
	prom_rules "github.com/prometheus/prometheus/rules"

	"github.com/timescale/promscale/pkg/log"
)

func Rules(conf *Config, updateMetrics func(handler, code string, duration float64)) http.Handler {
	hf := corsWrapper(conf, rulesHandler(conf, updateMetrics))
	return gziphandler.GzipHandler(hf)
}

// Below types and code is copied from prometheus/web/api.v1, starting from
// https://github.com/prometheus/prometheus/blob/854d671b6bc5d3ae7960936103ff23863c8a3f91/web/api/v1/api.go#L1168
// upto https://github.com/prometheus/prometheus/blob/854d671b6bc5d3ae7960936103ff23863c8a3f91/web/api/v1/api.go#L1288
//
// Note: We cannot directly import prometheus/web/api/v1 package and use these exported structs from there.
// This is because then we get duplicate enum registration in prompb, which is likely due to
// some dependency in api/v1 package. Thus, copying them here avoids the enum issue.

// RuleDiscovery has info for all rules
type RuleDiscovery struct {
	RuleGroups []*RuleGroup `json:"groups"`
}

// RuleGroup has info for rules which are part of a group
type RuleGroup struct {
	Name string `json:"name"`
	File string `json:"file"`
	// In order to preserve rule ordering, while exposing type (alerting or recording)
	// specific properties, both alerting and recording rules are exposed in the
	// same array.
	Rules          []Rule    `json:"rules"`
	Interval       float64   `json:"interval"`
	Limit          int       `json:"limit"`
	EvaluationTime float64   `json:"evaluationTime"`
	LastEvaluation time.Time `json:"lastEvaluation"`
}

type Rule interface{}

type AlertingRule struct {
	// State can be "pending", "firing", "inactive".
	State          string                `json:"state"`
	Name           string                `json:"name"`
	Query          string                `json:"query"`
	Duration       float64               `json:"duration"`
	Labels         labels.Labels         `json:"labels"`
	Annotations    labels.Labels         `json:"annotations"`
	Alerts         []*Alert              `json:"alerts"`
	Health         prom_rules.RuleHealth `json:"health"`
	LastError      string                `json:"lastError,omitempty"`
	EvaluationTime float64               `json:"evaluationTime"`
	LastEvaluation time.Time             `json:"lastEvaluation"`
	// Type of an alertingRule is always "alerting".
	Type string `json:"type"`
}

type RecordingRule struct {
	Name           string                `json:"name"`
	Query          string                `json:"query"`
	Labels         labels.Labels         `json:"labels,omitempty"`
	Health         prom_rules.RuleHealth `json:"health"`
	LastError      string                `json:"lastError,omitempty"`
	EvaluationTime float64               `json:"evaluationTime"`
	LastEvaluation time.Time             `json:"lastEvaluation"`
	// Type of a recordingRule is always "recording".
	Type string `json:"type"`
}

// AlertDiscovery has info for all active alerts.
type AlertDiscovery struct {
	Alerts []*Alert `json:"alerts"`
}

// Alert has info for an alert.
type Alert struct {
	Labels      labels.Labels `json:"labels"`
	Annotations labels.Labels `json:"annotations"`
	State       string        `json:"state"`
	ActiveAt    *time.Time    `json:"activeAt,omitempty"`
	Value       string        `json:"value"`
}

func rulesHandler(apiConf *Config, updateMetrics func(handler, code string, duration float64)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		statusCode := "400"
		begin := time.Now()
		defer func() {
			updateMetrics("/api/v1/rules", statusCode, time.Since(begin).Seconds())
		}()

		queryType := strings.ToLower(r.URL.Query().Get("type"))
		if queryType != "" && queryType != "alert" && queryType != "record" {
			log.Error("msg", "unsupported type", "type", queryType)
			respondError(w, http.StatusBadRequest, fmt.Errorf("unsupported type: type %s", queryType), "bad_data")
			return
		}

		if apiConf.Rules == nil {
			statusCode = "200"
			respond(w, http.StatusOK, &RuleDiscovery{RuleGroups: []*RuleGroup{}})
			return
		}

		returnAlerts := queryType == "" || queryType == "alert"
		returnRecording := queryType == "" || queryType == "record"

		ruleGroups := apiConf.Rules.RuleGroups()
		res := &RuleDiscovery{RuleGroups: make([]*RuleGroup, len(ruleGroups))}

		for i, grp := range ruleGroups {
			apiRuleGroup := &RuleGroup{
				Name:           grp.Name(),
				File:           grp.File(),
				Interval:       grp.Interval().Seconds(),
				Limit:          grp.Limit(),
				Rules:          []Rule{},
				EvaluationTime: grp.GetEvaluationTime().Seconds(),
				LastEvaluation: grp.GetLastEvaluation(),
			}
			for _, r := range grp.Rules() {
				var enrichedRule Rule

				lastError := ""
				if r.LastError() != nil {
					lastError = r.LastError().Error()
				}
				switch rule := r.(type) {
				case *prom_rules.AlertingRule:
					if !returnAlerts {
						break
					}
					enrichedRule = AlertingRule{
						State:          rule.State().String(),
						Name:           rule.Name(),
						Query:          rule.Query().String(),
						Duration:       rule.HoldDuration().Seconds(),
						Labels:         rule.Labels(),
						Annotations:    rule.Annotations(),
						Alerts:         rulesAlertsToAPIAlerts(rule.ActiveAlerts()),
						Health:         rule.Health(),
						LastError:      lastError,
						EvaluationTime: rule.GetEvaluationDuration().Seconds(),
						LastEvaluation: rule.GetEvaluationTimestamp(),
						Type:           "alerting",
					}
				case *prom_rules.RecordingRule:
					if !returnRecording {
						break
					}
					enrichedRule = RecordingRule{
						Name:           rule.Name(),
						Query:          rule.Query().String(),
						Labels:         rule.Labels(),
						Health:         rule.Health(),
						LastError:      lastError,
						EvaluationTime: rule.GetEvaluationDuration().Seconds(),
						LastEvaluation: rule.GetEvaluationTimestamp(),
						Type:           "recording",
					}
				default:
					statusCode = "500"
					err := fmt.Errorf("failed to assert type of rule '%v'", rule.Name())
					log.Error("msg", err.Error())
					respondError(w, http.StatusInternalServerError, err, "type_conversion")
					return
				}
				if enrichedRule != nil {
					apiRuleGroup.Rules = append(apiRuleGroup.Rules, enrichedRule)
				}
			}
			res.RuleGroups[i] = apiRuleGroup
		}
		statusCode = "200"
		respond(w, http.StatusOK, res)
	}
}

func rulesAlertsToAPIAlerts(rulesAlerts []*prom_rules.Alert) []*Alert {
	apiAlerts := make([]*Alert, len(rulesAlerts))
	for i, ruleAlert := range rulesAlerts {
		apiAlerts[i] = &Alert{
			Labels:      ruleAlert.Labels,
			Annotations: ruleAlert.Annotations,
			State:       ruleAlert.State.String(),
			ActiveAt:    &ruleAlert.ActiveAt,
			Value:       strconv.FormatFloat(ruleAlert.Value, 'e', -1, 64),
		}
	}

	return apiAlerts
}
