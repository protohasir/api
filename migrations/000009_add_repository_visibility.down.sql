DROP INDEX IF EXISTS idx_repositories_is_private;
ALTER TABLE repositories DROP COLUMN IF EXISTS description;
ALTER TABLE repositories DROP COLUMN IF EXISTS is_private;
