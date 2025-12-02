CREATE TYPE collaborator_permission AS ENUM ('read', 'write', 'admin');

CREATE TABLE IF NOT EXISTS repository_collaborators (
    id VARCHAR(36) PRIMARY KEY,
    repository_id VARCHAR(36) NOT NULL REFERENCES repositories(id) ON DELETE CASCADE,
    user_id VARCHAR(36) NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    permission collaborator_permission NOT NULL DEFAULT 'read',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    UNIQUE(repository_id, user_id)
);

CREATE INDEX idx_repository_collaborators_repo_id ON repository_collaborators(repository_id);
CREATE INDEX idx_repository_collaborators_user_id ON repository_collaborators(user_id);
