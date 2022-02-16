ALTER TABLE _ps_catalog.promscale_instance_information
  ADD COLUMN  promscale_ingested_spans_total BIGINT NOT NULL DEFAULT 0 ;