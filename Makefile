generate-mocks:
	mockgen -source=internal/user/service.go -package=user -destination=internal/user/service_mock.go
	mockgen -source=internal/user/repository.go -package=user -destination=internal/user/repository_mock.go
	mockgen -source=internal/registry/service.go -package=registry -destination=internal/registry/service_mock.go
	mockgen -source=internal/registry/repository.go -package=registry -destination=internal/registry/repository_mock.go
	mockgen -source=internal/organization/service.go -package=organization -destination=internal/organization/service_mock.go
	mockgen -source=internal/organization/repository.go -package=organization -destination=internal/organization/repository_mock.go
	mockgen -source=pkg/email/email.go -package=email -destination=pkg/email/email_mock.go

dev:
	MODE=development go run main.go

lint:
	golangci-lint run ./...

lint-fix:
	golangci-lint run --fix ./...
