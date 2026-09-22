-- Run in the Supabase SQL editor once. Safe to re-run.
-- Marks a movie as served from our own storage (R2/CDN) so the admin console
-- can sort self-hosted titles first.
ALTER TABLE moviesv2 ADD COLUMN IF NOT EXISTS hosted boolean NOT NULL DEFAULT false;

CREATE INDEX IF NOT EXISTS idx_moviesv2_hosted ON moviesv2 (hosted);

-- Optional but handy when filtering in the admin list.
-- NOTE: the column is stored case-sensitively as "modifiedAt" (PostgREST
-- created it from a JS payload). A plain `modifiedAt` in SQL folds to
-- lowercase `modifiedat` and fails with ERROR 42703. The DO block resolves
-- the real name and quotes it properly, so this file runs on any schema.
DO $$
DECLARE
  col TEXT;
BEGIN
  SELECT column_name INTO col
  FROM information_schema.columns
  WHERE table_schema = 'public'
    AND table_name = 'moviesv2'
    AND lower(column_name) = 'modifiedat';

  IF col IS NOT NULL THEN
    EXECUTE format(
      'CREATE INDEX IF NOT EXISTS idx_moviesv2_modified ON moviesv2 (%I DESC NULLS LAST)',
      col
    );
  END IF;
END
$$;