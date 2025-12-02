package gitserver

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	"github.com/gliderlabs/ssh"
	"go.uber.org/zap"
	gossh "golang.org/x/crypto/ssh"

	"hasir-api/pkg/config"
)

// SSHServer handles Git operations over SSH
type SSHServer struct {
	cfg      *config.Config
	server   *ssh.Server
	userRepo UserRepository
	repoRepo RepositoryRepository
	mu       sync.Mutex
}

// NewSSHServer creates a new SSH Git server
func NewSSHServer(cfg *config.Config, userRepo UserRepository, repoRepo RepositoryRepository) *SSHServer {
	return &SSHServer{
		cfg:      cfg,
		userRepo: userRepo,
		repoRepo: repoRepo,
	}
}

// Start starts the SSH server
func (s *SSHServer) Start() error {
	// Create repo root directory if it doesn't exist
	if err := os.MkdirAll(s.cfg.GitServer.RepoRootPath, 0755); err != nil {
		return fmt.Errorf("failed to create repo root directory: %w", err)
	}

	sshServer := &ssh.Server{
		Addr:    fmt.Sprintf(":%s", s.cfg.GitServer.SSHPort),
		Handler: s.handleSSHSession,
		PublicKeyHandler: func(ctx ssh.Context, key ssh.PublicKey) bool {
			return s.authenticatePublicKey(ctx, key)
		},
	}

	s.mu.Lock()
	s.server = sshServer
	s.mu.Unlock()

	zap.L().Info("SSH Git server starting", zap.String("port", s.cfg.GitServer.SSHPort))
	return sshServer.ListenAndServe()
}

// Shutdown gracefully shuts down the SSH server
func (s *SSHServer) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.server != nil {
		return s.server.Shutdown(ctx)
	}
	return nil
}

// authenticatePublicKey validates the user's public key
func (s *SSHServer) authenticatePublicKey(ctx ssh.Context, key ssh.PublicKey) bool {
	username := ctx.User()
	fingerprint := gossh.FingerprintSHA256(key)

	zap.L().Info("SSH authentication attempt",
		zap.String("username", username),
		zap.String("key_type", key.Type()),
		zap.String("fingerprint", fingerprint),
	)

	// Try to get user's public keys from database
	authCtx := context.Background()
	storedKeys, err := s.userRepo.GetUserPublicKeys(authCtx, username)
	if err != nil {
		zap.L().Error("Failed to retrieve user public keys",
			zap.String("username", username),
			zap.Error(err),
		)
		return false
	}

	// If user has no stored keys, deny access
	if len(storedKeys) == 0 {
		zap.L().Warn("User has no stored public keys",
			zap.String("username", username),
		)
		return false
	}

	// Compare provided key with stored keys
	providedKeyString := string(gossh.MarshalAuthorizedKey(key))
	for _, storedKey := range storedKeys {
		if strings.TrimSpace(providedKeyString) == strings.TrimSpace(storedKey) {
			zap.L().Info("SSH public key matched",
				zap.String("username", username),
				zap.String("fingerprint", fingerprint),
			)
			return true
		}
	}

	zap.L().Warn("SSH public key not found in database",
		zap.String("username", username),
		zap.String("fingerprint", fingerprint),
	)
	return false
}

// handleSSHSession handles Git commands over SSH
func (s *SSHServer) handleSSHSession(sess ssh.Session) {
	cmd := sess.Command()
	if len(cmd) < 2 {
		io.WriteString(sess, "Invalid command. Git usage required.\n")
		sess.Exit(1)
		return
	}

	// Git SSH commands format: git-upload-pack '/repo-name' or git-receive-pack '/repo-name'
	gitCommand := cmd[0]  // e.g., git-upload-pack (pull) or git-receive-pack (push)
	rawRepoName := cmd[1] // e.g., '/owner/repo-name'

	// Clean quotes and leading slash
	repoName := strings.Trim(rawRepoName, "'/")

	zap.L().Info("Git SSH command received",
		zap.String("command", gitCommand),
		zap.String("repo", repoName),
		zap.String("user", sess.User()),
	)

	// Parse repository path (format: owner/repo-name)
	parts := strings.Split(repoName, "/")
	if len(parts) < 2 {
		io.WriteString(sess, "Invalid repository path. Format: owner/repo-name\n")
		sess.Exit(1)
		return
	}

	owner := parts[0]
	repo := strings.Join(parts[1:], "/")

	// Build full repository path
	repoPath := filepath.Join(s.cfg.GitServer.RepoRootPath, owner, repo)

	// Check if repository exists
	if _, err := os.Stat(repoPath); os.IsNotExist(err) {
		io.WriteString(sess, "Repository not found.\n")
		sess.Exit(1)
		return
	}

	// Check user permissions
	ctx := context.Background()
	accessType := "read"
	if gitCommand == "git-receive-pack" {
		accessType = "write"
	}

	hasAccess, err := s.repoRepo.CheckRepositoryAccess(ctx, sess.User(), owner, repo, accessType)
	if err != nil || !hasAccess {
		zap.L().Warn("Access denied",
			zap.String("user", sess.User()),
			zap.String("repo", repoName),
			zap.String("access_type", accessType),
			zap.Error(err),
		)
		io.WriteString(sess, "Access denied.\n")
		sess.Exit(1)
		return
	}

	// Extract git command (git-upload-pack -> upload-pack)
	gitCmd := gitCommand
	if strings.HasPrefix(gitCommand, "git-") {
		gitCmd = gitCommand[4:]
	}

	// Execute git command
	sysCmd := exec.Command("git", gitCmd, ".")
	sysCmd.Dir = repoPath
	sysCmd.Stdout = sess
	sysCmd.Stdin = sess
	sysCmd.Stderr = sess.Stderr()

	if err := sysCmd.Run(); err != nil {
		zap.L().Error("Git SSH command error",
			zap.Error(err),
			zap.String("command", gitCommand),
			zap.String("repo", repoName),
		)
		sess.Exit(1)
		return
	}

	sess.Exit(0)
}
