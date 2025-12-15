package sdkgenerator

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBufGenerator_IsApplicable(t *testing.T) {
	tempDir := t.TempDir()
	generator := NewBufGenerator(NewMockCommandRunner())

	t.Run("buf.gen.yaml exists", func(t *testing.T) {
		bufGenYamlPath := filepath.Join(tempDir, "buf.gen.yaml")
		err := os.WriteFile(bufGenYamlPath, []byte("version: v1\n"), 0o644)
		require.NoError(t, err)

		assert.True(t, generator.IsApplicable(tempDir))
	})

	t.Run("buf.gen.yaml does not exist", func(t *testing.T) {
		emptyDir := t.TempDir()
		assert.False(t, generator.IsApplicable(emptyDir))
	})

	t.Run("non-existent directory", func(t *testing.T) {
		assert.False(t, generator.IsApplicable("/nonexistent/path"))
	})
}

func TestBufGenerator_Validate(t *testing.T) {
	tempDir := t.TempDir()
	generator := NewBufGenerator(NewMockCommandRunner())

	t.Run("valid with buf.gen.yaml", func(t *testing.T) {
		bufGenYamlPath := filepath.Join(tempDir, "buf.gen.yaml")
		err := os.WriteFile(bufGenYamlPath, []byte("version: v1\n"), 0o644)
		require.NoError(t, err)

		input := GeneratorInput{
			RepoPath:   tempDir,
			OutputPath: "/tmp/output",
			ProtoFiles: []string{},
		}

		err = generator.Validate(input)
		assert.NoError(t, err)
	})

	t.Run("invalid without buf.gen.yaml", func(t *testing.T) {
		emptyDir := t.TempDir()
		input := GeneratorInput{
			RepoPath:   emptyDir,
			OutputPath: "/tmp/output",
			ProtoFiles: []string{},
		}

		err := generator.Validate(input)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "buf.gen.yaml not found")
	})
}

func TestBufGenerator_Generate(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	outputDir := t.TempDir()

	bufGenYamlPath := filepath.Join(tempDir, "buf.gen.yaml")
	bufGenYamlContent := `version: v1
plugins:
  - plugin: buf.build/protocolbuffers/go
    out: gen/go
    opt: paths=source_relative
`
	err := os.WriteFile(bufGenYamlPath, []byte(bufGenYamlContent), 0o644)
	require.NoError(t, err)

	protoDir := filepath.Join(tempDir, "proto")
	err = os.MkdirAll(protoDir, 0o750)
	require.NoError(t, err)
	protoFile := filepath.Join(protoDir, "test.proto")
	err = os.WriteFile(protoFile, []byte(`syntax = "proto3";
package test;
message TestMessage {
  string name = 1;
}`), 0o644)
	require.NoError(t, err)

	t.Run("successful generation", func(t *testing.T) {
		mockRunner := NewMockCommandRunner()
		mockRunner.RunFunc = func(ctx context.Context, name string, args []string, workDir string) ([]byte, error) {
			// Simulate buf generate creating files
			genDir := filepath.Join(workDir, "gen", "go", "proto")
			err := os.MkdirAll(genDir, 0o750)
			if err != nil {
				return nil, err
			}
			generatedFile := filepath.Join(genDir, "test.pb.go")
			err = os.WriteFile(generatedFile, []byte("package proto\n"), 0o644)
			return []byte("Success"), err
		}

		generator := NewBufGenerator(mockRunner)
		input := GeneratorInput{
			RepoPath:   tempDir,
			OutputPath: outputDir,
			ProtoFiles: []string{},
		}

		output, err := generator.Generate(ctx, input)
		require.NoError(t, err)
		assert.NotNil(t, output)
		assert.Equal(t, outputDir, output.OutputPath)
		assert.Greater(t, output.FilesCount, 0)

		// Verify buf generate was called
		assert.Len(t, mockRunner.Calls, 1)
		call := mockRunner.Calls[0]
		assert.Equal(t, "buf", call.Name)
		assert.Equal(t, []string{"generate"}, call.Args)
		assert.Equal(t, tempDir, call.WorkDir)
	})

	t.Run("buf generate fails", func(t *testing.T) {
		mockRunner := NewMockCommandRunner()
		mockRunner.RunFunc = func(ctx context.Context, name string, args []string, workDir string) ([]byte, error) {
			return []byte("error output"), errors.New("buf generate failed")
		}

		generator := NewBufGenerator(mockRunner)
		input := GeneratorInput{
			RepoPath:   tempDir,
			OutputPath: outputDir,
			ProtoFiles: []string{},
		}

		output, err := generator.Generate(ctx, input)
		require.Error(t, err)
		assert.Nil(t, output)
		assert.Contains(t, err.Error(), "buf generate failed")
	})

	t.Run("validation fails", func(t *testing.T) {
		emptyDir := t.TempDir()
		mockRunner := NewMockCommandRunner()
		generator := NewBufGenerator(mockRunner)

		input := GeneratorInput{
			RepoPath:   emptyDir,
			OutputPath: outputDir,
			ProtoFiles: []string{},
		}

		output, err := generator.Generate(ctx, input)
		require.Error(t, err)
		assert.Nil(t, output)
		assert.Contains(t, err.Error(), "buf.gen.yaml not found")
		assert.Empty(t, mockRunner.Calls)
	})
}

func TestBufGenerator_findGeneratedFiles(t *testing.T) {
	tempDir := t.TempDir()
	generator := NewBufGenerator(NewMockCommandRunner())

	genDir := filepath.Join(tempDir, "gen", "go")
	err := os.MkdirAll(genDir, 0o750)
	require.NoError(t, err)

	generatedFile1 := filepath.Join(genDir, "test.pb.go")
	err = os.WriteFile(generatedFile1, []byte("package go\n"), 0o644)
	require.NoError(t, err)

	generatedFile2 := filepath.Join(genDir, "test_grpc.pb.go")
	err = os.WriteFile(generatedFile2, []byte("package go\n"), 0o644)
	require.NoError(t, err)

	protoFile := filepath.Join(tempDir, "test.proto")
	err = os.WriteFile(protoFile, []byte("syntax = \"proto3\";\n"), 0o644)
	require.NoError(t, err)

	bufGenYaml := filepath.Join(tempDir, "buf.gen.yaml")
	err = os.WriteFile(bufGenYaml, []byte("version: v1\n"), 0o644)
	require.NoError(t, err)

	bufYaml := filepath.Join(tempDir, "buf.yaml")
	err = os.WriteFile(bufYaml, []byte("version: v1\n"), 0o644)
	require.NoError(t, err)

	files, err := generator.findGeneratedFiles(tempDir)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(files), 2)

	for _, file := range files {
		assert.NotEqual(t, "test.proto", file)
		assert.NotEqual(t, "buf.gen.yaml", file)
		assert.NotEqual(t, "buf.yaml", file)
		assert.False(t, filepath.Ext(file) == ".proto", "file %s should not be a proto file", file)
	}
}

func TestBufGenerator_copyGeneratedFiles(t *testing.T) {
	tempDir := t.TempDir()
	outputDir := t.TempDir()
	generator := NewBufGenerator(NewMockCommandRunner())

	// Create source files
	srcDir := filepath.Join(tempDir, "gen", "go")
	err := os.MkdirAll(srcDir, 0o750)
	require.NoError(t, err)

	file1 := filepath.Join(srcDir, "test.pb.go")
	err = os.WriteFile(file1, []byte("package go\n// Generated code\n"), 0o644)
	require.NoError(t, err)

	file2 := filepath.Join(srcDir, "nested", "helper.go")
	err = os.MkdirAll(filepath.Dir(file2), 0o750)
	require.NoError(t, err)
	err = os.WriteFile(file2, []byte("package nested\n"), 0o644)
	require.NoError(t, err)

	generatedFiles := []string{
		"gen/go/test.pb.go",
		"gen/go/nested/helper.go",
	}

	err = generator.copyGeneratedFiles(tempDir, outputDir, generatedFiles)
	require.NoError(t, err)

	// Verify files were copied
	dstFile1 := filepath.Join(outputDir, "gen/go/test.pb.go")
	assert.FileExists(t, dstFile1)
	content1, err := os.ReadFile(dstFile1)
	require.NoError(t, err)
	assert.Equal(t, "package go\n// Generated code\n", string(content1))

	dstFile2 := filepath.Join(outputDir, "gen/go/nested/helper.go")
	assert.FileExists(t, dstFile2)
	content2, err := os.ReadFile(dstFile2)
	require.NoError(t, err)
	assert.Equal(t, "package nested\n", string(content2))
}

func TestBufGenerator_SDK(t *testing.T) {
	generator := NewBufGenerator(NewMockCommandRunner())
	assert.Equal(t, SdkBuf, generator.SDK())
}

func TestBufGenerator_DirName(t *testing.T) {
	generator := NewBufGenerator(NewMockCommandRunner())
	assert.Equal(t, "buf", generator.DirName())
}
