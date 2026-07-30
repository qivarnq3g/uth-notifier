ALTER TABLE sources
ADD COLUMN unchanged_crawl_count INTEGER NOT NULL DEFAULT 6
CHECK (unchanged_crawl_count >= 0);
