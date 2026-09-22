-- Run in the Supabase SQL editor once.
-- Marks a movie as served from our own storage (R2/CDN) so the admin console
-- can sort self-hosted titles first.
ALTER TABLE moviesv2 ADD COLUMN IF NOT EXISTS hosted boolean NOT NULL DEFAULT false;

CREATE INDEX IF NOT EXISTS idx_moviesv2_hosted ON moviesv2 (hosted);

-- Optional but handy when filtering in the admin list.
CREATE INDEX IF NOT EXISTS idx_moviesv2_modified ON moviesv2 (modifiedAt DESC NULLS LAST);