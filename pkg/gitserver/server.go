package gitserver

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	"go.uber.org/zap"

	"hasir-api/pkg/config"
)

// Server manages both SSH and HTTP Git servers
type Server struct {
	cfg        *config.Config
	sshServer  *SSHServer
	httpServer *HTTPServer
	wg         sync.WaitGroup
	mu         sync.Mutex
	started    bool
}

// NewServer creates a new Git server instance
func NewServer(cfg *config.Config, userRepo UserRepository, repoRepo RepositoryRepository) (*Server, error) {
	if !cfg.GitServer.Enabled {
		return nil, nil
	}

	sshServer := NewSSHServer(cfg, userRepo, repoRepo)
	httpServer := NewHTTPServer(cfg, userRepo, repoRepo)

	return &Server{
		cfg:        cfg,
		sshServer:  sshServer,
		httpServer: httpServer,
	}, nil
}

// Start starts both SSH and HTTP Git servers
func (s *Server) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.started {
		return fmt.Errorf("git servers already started")
	}

	// Start SSH server
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.sshServer.Start(); err != nil && err != http.ErrServerClosed {
			zap.L().Error("SSH Git server error", zap.Error(err))
		}
	}()

	// Start HTTP server
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.httpServer.Start(); err != nil && err != http.ErrServerClosed {
			zap.L().Error("HTTP Git server error", zap.Error(err))
		}
	}()

	s.started = true
	zap.L().Info("Git servers started",
		zap.String("ssh_port", s.cfg.GitServer.SSHPort),
		zap.String("http_port", s.cfg.GitServer.HTTPPort),
	)

	return nil
}

// Shutdown gracefully shuts down both servers
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started {
		return nil
	}

	zap.L().Info("Shutting down Git servers...")

	// Shutdown SSH server
	if err := s.sshServer.Shutdown(ctx); err != nil {
		zap.L().Error("SSH server shutdown error", zap.Error(err))
	}

	// Shutdown HTTP server
	if err := s.httpServer.Shutdown(ctx); err != nil {
		zap.L().Error("HTTP Git server shutdown error", zap.Error(err))
	}

	s.wg.Wait()
	s.started = false

	zap.L().Info("Git servers stopped")
	return nil
}
