-- the migrations runs on every shard database, and each shard stores the subset of urls table which independent of each other 


CREATE TABLE IF NOT EXISTS urls (
   short_code  VARCHAR(12)  PRIMARY KEY,
   long_url TEXT NOT NULL,
   user_id BIGINT NOT NULL DEFAULT 0,
   created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
   expries_at TIMESTAMPTZ,
   click_count BIGINT NOT NULL DEFAULT 0 
);


-- index to get all links for a user 

CREATE INDEX IF NOT EXISTS idx_urls_user_id ON urls (user_id);


-- in order for user according to creation time

CREATE INDEX IF NOT EXISTS idx_urls_created_at ON urls (created_at DESC);


-- index to find expiry links for cleanup 

CREATE INDEX IF NOT EXISTS idx_urls_expires_at ON urls (expires_at)
    WHERE expires_at IS NOT NULL;
    

