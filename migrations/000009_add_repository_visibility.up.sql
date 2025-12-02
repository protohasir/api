-- Add visibility column to repositories
ALTER TABLE repositories 
ADD COLUMN is_private BOOLEAN NOT NULL DEFAULT true;

-- Add description column
ALTER TABLE repositories 
ADD COLUMN description TEXT;

CREATE INDEX idx_repositories_is_private ON repositories(is_private);
