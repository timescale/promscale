// Promscale stores some configuration information in the Postgres database
// which it is connected to. We call this configuration the Promscale dataset
// configuration.
//
// The dataset configuration handles config items like retention period,
// chunk interval, compression, etc. This config options are applied on startup
// as SQL queries. For more context review the `/docs/dataset.md` page and the
// `dataset/config.go` file.
//
// +k8s:deepcopy-gen=package
package dataset
