package gitserver

import (
	"context"
	"fmt"
	"net/http"
	"net/http/cgi"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	"go.uber.org/zap"

	"hasir-api/pkg/config"
)

// HTTPServer handles Git operations over HTTP using git-http-backend
type HTTPServer struct {
	cfg      *config.Config
	server   *http.Server
	userRepo UserRepository
	repoRepo RepositoryRepository
	mu       sync.Mutex
}

// NewHTTPServer creates a new HTTP Git server
func NewHTTPServer(cfg *config.Config, userRepo UserRepository, repoRepo RepositoryRepository) *HTTPServer {
	return &HTTPServer{
		cfg:      cfg,
		userRepo: userRepo,
		repoRepo: repoRepo,
	}
}

// Start starts the HTTP Git server
func (s *HTTPServer) Start() error {
	// Create repo root directory if it doesn't exist
	if err := os.MkdirAll(s.cfg.GitServer.RepoRootPath, 0755); err != nil {
		return fmt.Errorf("failed to create repo root directory: %w", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleGitHTTP)

	// Create HTTP server
	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%s", s.cfg.GitServer.HTTPPort),
		Handler: mux,
	}

	s.mu.Lock()
	s.server = httpServer
	s.mu.Unlock()

	zap.L().Info("HTTP Git server starting", zap.String("port", s.cfg.GitServer.HTTPPort))
	return httpServer.ListenAndServe()
}

// Shutdown gracefully shuts down the HTTP server
func (s *HTTPServer) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.server != nil {
		return s.server.Shutdown(ctx)
	}
	return nil
}

// handleGitHTTP handles Git HTTP requests using git-http-backend
func (s *HTTPServer) handleGitHTTP(w http.ResponseWriter, r *http.Request) {
	// Basic authentication
	username, password, ok := r.BasicAuth()
	if !ok {
		w.Header().Set("WWW-Authenticate", `Basic realm="Git Repository"`)
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	// Authenticate user
	ctx := context.Background()
	valid, err := s.userRepo.ValidateUserPassword(ctx, username, password)
	if err != nil || !valid {
		zap.L().Warn("HTTP authentication failed",
			zap.String("username", username),
			zap.Error(err),
		)
		w.Header().Set("WWW-Authenticate", `Basic realm="Git Repository"`)
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	// Parse repository path from URL
	// Expected format: /owner/repo.git/...
	pathParts := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
	if len(pathParts) < 2 {
		http.Error(w, "Invalid repository path", http.StatusBadRequest)
		return
	}

	owner := pathParts[0]
	repoWithGit := pathParts[1]
	repo := strings.TrimSuffix(repoWithGit, ".git")

	repoPath := filepath.Join(s.cfg.GitServer.RepoRootPath, owner, repo)

	// Check if repository exists
	if _, err := os.Stat(repoPath); os.IsNotExist(err) {
		http.Error(w, "Repository not found", http.StatusNotFound)
		return
	}

	// Check permissions based on HTTP method/service
	accessType := "read"
	if r.Method == "POST" || strings.Contains(r.URL.Path, "git-receive-pack") {
		accessType = "write"
	}

	hasAccess, err := s.repoRepo.CheckRepositoryAccess(ctx, username, owner, repo, accessType)
	if err != nil || !hasAccess {
		zap.L().Warn("Access denied",
			zap.String("user", username),
			zap.String("repo", fmt.Sprintf("%s/%s", owner, repo)),
			zap.String("access_type", accessType),
			zap.Error(err),
		)
		http.Error(w, "Access denied", http.StatusForbidden)
		return
	}

	// Use git-http-backend via CGI
	// Note: This requires git to be installed on the system
	gitPath, err := exec.LookPath("git")
	if err != nil {
		zap.L().Error("Git binary not found", zap.Error(err))
		http.Error(w, "Git not configured on server", http.StatusInternalServerError)
		return
	}

	// Set up CGI handler for git-http-backend
	handler := &cgi.Handler{
		Path: gitPath,
		Args: []string{"http-backend"},
		Env: []string{
			fmt.Sprintf("GIT_PROJECT_ROOT=%s", filepath.Join(s.cfg.GitServer.RepoRootPath, owner)),
			"GIT_HTTP_EXPORT_ALL=1",
			fmt.Sprintf("PATH_INFO=/%s.git%s", repo, strings.Join(pathParts[2:], "/")),
		},
	}

	handler.ServeHTTP(w, r)
}
