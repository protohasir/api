package sdkgenerator

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type BufGenerator struct {
	*baseGenerator
}

func NewBufGenerator(runner CommandRunner) *BufGenerator {
	return &BufGenerator{
		baseGenerator: &baseGenerator{
			sdk:     SdkBuf,
			dirName: "buf",
			runner:  runner,
		},
	}
}

func (g *BufGenerator) IsApplicable(repoPath string) bool {
	bufGenYamlPath := filepath.Join(repoPath, "buf.gen.yaml")
	_, err := os.Stat(bufGenYamlPath)
	return err == nil
}

func (g *BufGenerator) Validate(input GeneratorInput) error {
	if !g.IsApplicable(input.RepoPath) {
		return errors.New("buf.gen.yaml not found in repository")
	}
	return nil
}

func (g *BufGenerator) Generate(ctx context.Context, input GeneratorInput) (*GeneratorOutput, error) {
	if err := g.Validate(input); err != nil {
		return nil, err
	}

	args := []string{"generate"}
	output, err := g.runner.Run(ctx, "buf", args, input.RepoPath)
	if err != nil {
		return nil, fmt.Errorf("buf generate failed: %w: %s", err, string(output))
	}

	generatedFiles, err := g.findGeneratedFiles(input.RepoPath)
	if err != nil {
		return nil, fmt.Errorf("failed to find generated files: %w", err)
	}

	if err := g.copyGeneratedFiles(input.RepoPath, input.OutputPath, generatedFiles); err != nil {
		return nil, fmt.Errorf("failed to copy generated files: %w", err)
	}

	return &GeneratorOutput{
		OutputPath: input.OutputPath,
		FilesCount: len(generatedFiles),
	}, nil
}

func (g *BufGenerator) findGeneratedFiles(repoPath string) ([]string, error) {
	var generatedFiles []string
	err := filepath.Walk(repoPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			if info.Name() == ".git" {
				return filepath.SkipDir
			}
			return nil
		}

		relPath, err := filepath.Rel(repoPath, path)
		if err != nil {
			return nil
		}

		if relPath == "buf.gen.yaml" || filepath.Ext(path) == ".proto" {
			return nil
		}

		if strings.HasPrefix(relPath, "buf.") && strings.HasSuffix(relPath, ".yaml") {
			return nil
		}

		generatedFiles = append(generatedFiles, relPath)
		return nil
	})

	return generatedFiles, err
}

func (g *BufGenerator) copyGeneratedFiles(repoPath, outputPath string, generatedFiles []string) error {
	absOutputPath, err := filepath.Abs(outputPath)
	if err != nil {
		return fmt.Errorf("failed to get absolute output path: %w", err)
	}

	if err := os.MkdirAll(absOutputPath, 0o750); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	for _, relPath := range generatedFiles {
		srcPath := filepath.Join(repoPath, relPath)
		dstPath := filepath.Join(absOutputPath, relPath)

		if err := os.MkdirAll(filepath.Dir(dstPath), 0o750); err != nil {
			return fmt.Errorf("failed to create destination directory: %w", err)
		}

		srcData, err := os.ReadFile(srcPath)
		if err != nil {
			return fmt.Errorf("failed to read source file: %w", err)
		}

		if err := os.WriteFile(dstPath, srcData, 0o644); err != nil {
			return fmt.Errorf("failed to write destination file: %w", err)
		}
	}

	return nil
}
