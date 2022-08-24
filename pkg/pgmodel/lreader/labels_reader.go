// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package lreader

import (
	"context"
	"fmt"
	"sort"
	"unsafe"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/pgmodel/model/pgutf8str"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/tenancy"
)

const (
	getLabelNamesSQL  = "SELECT distinct key from _prom_catalog.label"
	getLabelValuesSQL = "SELECT value from _prom_catalog.label WHERE key = $1"
	getLabelsSQL      = "SELECT (prom_api.labels_info($1::int[])).*"
)

// LabelsReader defines the methods for accessing labels data
type LabelsReader interface {
	// LabelNames returns all the distinct label names in the system.
	LabelNames() ([]string, error)
	// LabelValues returns all the distinct values for a given label name.
	LabelValues(labelName string) ([]string, error)
	// LabelsForIdMap fills in the label.Label values in a map of label id => labels.Label.
	LabelsForIdMap(idMap map[int64]labels.Label) (err error)
}

func NewLabelsReader(conn pgxconn.PgxConn, labels cache.LabelsCache, mt tenancy.ReadAuthorizer) LabelsReader {
	var authConfig tenancy.AuthConfig
	if mt != nil {
		authConfig = mt.(tenancy.AuthConfig)
	}
	return &labelsReader{conn: conn, labels: labels, authConfig: authConfig}
}

type labelsReader struct {
	conn       pgxconn.PgxConn
	labels     cache.LabelsCache
	authConfig tenancy.AuthConfig
}

// LabelValues implements the LabelsReader interface. It returns all distinct values
// for a specified label name.
func (lr *labelsReader) LabelValues(labelName string) ([]string, error) {
	rows, err := lr.conn.Query(context.Background(), getLabelValuesSQL, labelName)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	labelValues := make([]string, 0)

	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}
		if labelName == tenancy.TenantLabelKey && lr.authConfig != nil {
			if lr.authConfig.IsTenantAllowed(value) {
				labelValues = append(labelValues, value)
			}
		} else {
			labelValues = append(labelValues, value)
		}
	}

	sort.Strings(labelValues)
	return labelValues, nil
}

const getLabelNamesBelongingToTenantNames = `
SELECT ARRAY_AGG(a.key) FROM
(
	SELECT 
	  key 
	FROM 
	  _prom_catalog.label WHERE id IN (
		SELECT 
		  UNNEST(labels) as valid_label_ids 
		FROM 
		  "_prom_catalog".series 
		WHERE 
		  labels && (SELECT array_agg(id :: INTEGER) FROM _prom_catalog.label WHERE key = '__tenant__' AND value = ANY($1)) -- List of valid tenant names goes here.
	  ) GROUP BY 1 ORDER BY 1
) a
`

// LabelNames implements the LabelReader interface. It returns all distinct
// label names available in the database.
func (lr *labelsReader) LabelNames() ([]string, error) {
	if lr.authConfig != nil {
		// Multi-tenancy is enabled. Hence, we have to use a different LabelNames() query
		// only when some tenants are authorized, i.e., when using NewSelectiveTenancyConfig().
		//
		// Note: Label names of non-tenants will not be sent. Only label names belonging to
		// authorized tenants will be sent. This means, if -metrics.multi-tenancy.allow-non-tenants
		// is enabled, the non-tenant labels will not be answered ATM.
		validTenants, allTenantsValid := lr.authConfig.ValidTenants()
		if len(validTenants) == 0 {
			return []string{}, nil
		}
		if !allTenantsValid {
			var labelNames []string
			if err := lr.conn.QueryRow(context.Background(), getLabelNamesBelongingToTenantNames, validTenants).Scan(&labelNames); err != nil {
				return nil, fmt.Errorf("error reading label names belonging to a tenant id: %w", err)
			}
			if labelNames == nil {
				labelNames = []string{}
			}
			return labelNames, nil
		}
	}

	rows, err := lr.conn.Query(context.Background(), getLabelNamesSQL)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	labelNames := make([]string, 0)

	for rows.Next() {
		var labelName string
		if err := rows.Scan(&labelName); err != nil {
			return nil, err
		}

		labelNames = append(labelNames, labelName)
	}

	sort.Strings(labelNames)
	return labelNames, nil
}

// LabelsForIdMap fills in the label.Label values in a map of label id => labels.Label.
func (lr *labelsReader) LabelsForIdMap(idMap map[int64]labels.Label) error {
	numIds := len(idMap)
	ids := make([]interface{}, numIds) //type int64
	lbs := make([]interface{}, numIds) //type labels.Label

	//id=0 reserved for "no label for key" so this lookup would always fail. Should
	//never have been passed in with the idMap to begin with.
	_, present := idMap[0]
	if present {
		return fmt.Errorf("looking up a label for id 0")
	}

	i := 0
	for id := range idMap {
		ids[i] = id
		i++
	}
	numHits := lr.labels.GetValues(ids, lbs)

	if numHits < numIds {
		var (
			numFetches int
			err        error
		)

		missingIds := make([]int64, numIds-numHits)
		numFetches, err = lr.fetchMissingLabels(ids[numHits:], missingIds, lbs[numHits:])
		if err != nil {
			return err
		}
		if numFetches+numHits != numIds {
			return fmt.Errorf("missing labels: total %v, fetches %v hits %v", numIds, numFetches, numHits)
		}
	}

	for i := range ids {
		label := lbs[i].(labels.Label)
		id := ids[i].(int64)
		idMap[id] = label
	}

	return nil
}

// fetchMissingLabels imports the missing label IDs from the database into the
// internal cache. It also modifies the newLabels slice to include the missing
// values.
func (lr *labelsReader) fetchMissingLabels(misses []interface{}, missedIds []int64, newLabels []interface{}) (numNewLabels int, err error) {
	for i := range misses {
		missedIds[i] = misses[i].(int64)
	}
	rows, err := lr.conn.Query(context.Background(), getLabelsSQL, missedIds)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var (
		keys pgutf8str.TextArray
		vals pgutf8str.TextArray
	)

	for rows.Next() {
		var ids []int64
		err = rows.Scan(&ids, &keys, &vals)
		if err != nil {
			return 0, err
		}
		if len(ids) != len(keys.Elements) {
			return 0, fmt.Errorf("query returned a mismatch in ids and keys: %d, %d", len(ids), len(keys.Elements))
		}
		if len(keys.Elements) != len(vals.Elements) {
			return 0, fmt.Errorf("query returned a mismatch in timestamps and values: %d, %d", len(keys.Elements), len(vals.Elements))
		}
		if len(keys.Elements) > len(misses) {
			return 0, fmt.Errorf("query returned wrong number of labels: %d, %d", len(misses), len(keys.Elements))
		}

		numNewLabels = len(keys.Elements)
		misses = misses[:len(keys.Elements)]
		newLabels = newLabels[:len(keys.Elements)]
		keyStrArr := keys.Get().([]string)
		valStrArr := vals.Get().([]string)
		sizes := make([]uint64, numNewLabels)
		for i := range newLabels {
			misses[i] = ids[i]
			newLabels[i] = labels.Label{Name: keyStrArr[i], Value: valStrArr[i]}
			sizes[i] = uint64(8 + int(unsafe.Sizeof(labels.Label{})) + len(keyStrArr[i]) + len(valStrArr[i])) // #nosec
		}

		numInserted := lr.labels.InsertBatch(misses, newLabels, sizes)
		if numInserted < len(misses) {
			log.Warn("msg", "labels cache starving, may need to increase size")
		}
	}
	return numNewLabels, nil
}
