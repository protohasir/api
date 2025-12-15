package sdkgenerator

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewRegistry(t *testing.T) {
	r := NewRegistry(nil)
	assert.NotNil(t, r)

	expectedSdks := []SDK{
		SdkBuf,
		SdkGoProtobuf,
		SdkGoConnectRpc,
		SdkGoGrpc,
		SdkJsBufbuildEs,
		SdkJsProtobuf,
		SdkJsConnectrpc,
	}

	for _, sdk := range expectedSdks {
		g, err := r.Get(sdk)
		require.NoError(t, err)
		assert.NotNil(t, g)
		assert.Equal(t, sdk, g.SDK())
	}
}

func TestRegistry_Get(t *testing.T) {
	r := NewRegistry(NewMockCommandRunner())

	t.Run("existing generator", func(t *testing.T) {
		g, err := r.Get(SdkGoProtobuf)
		require.NoError(t, err)
		assert.NotNil(t, g)
		assert.Equal(t, SdkGoProtobuf, g.SDK())
	})

	t.Run("non-existing generator", func(t *testing.T) {
		g, err := r.Get(SDK("UNKNOWN"))
		require.Error(t, err)
		assert.Nil(t, g)
		assert.Contains(t, err.Error(), "no generator registered")
	})
}

func TestRegistry_List(t *testing.T) {
	r := NewRegistry(NewMockCommandRunner())

	sdks := r.List()
	assert.Len(t, sdks, 7)

	sdkMap := make(map[SDK]bool)
	for _, sdk := range sdks {
		sdkMap[sdk] = true
	}

	assert.True(t, sdkMap[SdkBuf])
	assert.True(t, sdkMap[SdkGoProtobuf])
	assert.True(t, sdkMap[SdkGoConnectRpc])
	assert.True(t, sdkMap[SdkGoGrpc])
	assert.True(t, sdkMap[SdkJsBufbuildEs])
	assert.True(t, sdkMap[SdkJsProtobuf])
	assert.True(t, sdkMap[SdkJsConnectrpc])
}

func TestRegistry_Register(t *testing.T) {
	r := NewRegistry(NewMockCommandRunner())

	mockGenerator := &mockGenerator{sdk: SDK("CUSTOM")}
	r.Register(mockGenerator)

	g, err := r.Get(SDK("CUSTOM"))
	require.NoError(t, err)
	assert.Equal(t, SDK("CUSTOM"), g.SDK())
}

type mockGenerator struct {
	sdk     SDK
	dirName string
}

func (m *mockGenerator) Generate(_ context.Context, _ GeneratorInput) (*GeneratorOutput, error) {
	return &GeneratorOutput{}, nil
}

func (m *mockGenerator) SDK() SDK {
	return m.sdk
}

func (m *mockGenerator) DirName() string {
	if m.dirName != "" {
		return m.dirName
	}
	return "custom"
}

func (m *mockGenerator) Validate(_ GeneratorInput) error {
	return nil
}

func (m *mockGenerator) IsApplicable(_ string) bool {
	return true
}

func TestRegistryBuilder(t *testing.T) {
	runner := NewMockCommandRunner()

	t.Run("build registry with custom generators", func(t *testing.T) {
		customGen := &mockGenerator{sdk: SDK("CUSTOM"), dirName: "custom-sdk"}

		r := NewRegistryBuilder(runner).
			WithGenerator(NewGoProtobufGenerator(runner)).
			WithGenerator(customGen).
			Build()

		g1, err := r.Get(SdkGoProtobuf)
		require.NoError(t, err)
		assert.NotNil(t, g1)

		g2, err := r.Get(SDK("CUSTOM"))
		require.NoError(t, err)
		assert.NotNil(t, g2)
		assert.Equal(t, "custom-sdk", g2.DirName())
	})

	t.Run("build registry with default generators", func(t *testing.T) {
		r := NewRegistryBuilder(runner).
			WithDefaultGenerators().
			Build()

		sdks := r.List()
		assert.Len(t, sdks, 7)
	})

	t.Run("build registry with mix of default and custom", func(t *testing.T) {
		customGen := &mockGenerator{sdk: SDK("CUSTOM"), dirName: "custom"}

		r := NewRegistryBuilder(runner).
			WithDefaultGenerators().
			WithGenerator(customGen).
			Build()

		sdks := r.List()
		assert.Len(t, sdks, 8) // 7 defaults + 1 custom

		g, err := r.Get(SDK("CUSTOM"))
		require.NoError(t, err)
		assert.Equal(t, "custom", g.DirName())
	})
}

func TestRegistry_FindApplicableGenerator(t *testing.T) {
	runner := NewMockCommandRunner()
	r := NewRegistry(runner)

	t.Run("finds buf generator when buf.gen.yaml exists", func(t *testing.T) {
		tempDir := t.TempDir()
		bufGenYamlPath := filepath.Join(tempDir, "buf.gen.yaml")
		err := os.WriteFile(bufGenYamlPath, []byte("version: v1\n"), 0o644)
		require.NoError(t, err)

		generator := r.FindApplicableGenerator(tempDir)
		require.NotNil(t, generator)
		assert.Equal(t, SdkBuf, generator.SDK())
	})

	t.Run("finds protoc generator when buf.gen.yaml does not exist but proto files exist", func(t *testing.T) {
		tempDir := t.TempDir()
		protoFile := filepath.Join(tempDir, "test.proto")
		err := os.WriteFile(protoFile, []byte("syntax = \"proto3\";\n"), 0o644)
		require.NoError(t, err)

		generator := r.FindApplicableGenerator(tempDir)
		require.NotNil(t, generator)
		assert.NotEqual(t, SdkBuf, generator.SDK())
	})

	t.Run("returns nil when no generators are applicable", func(t *testing.T) {
		emptyDir := t.TempDir()

		generator := r.FindApplicableGenerator(emptyDir)
		assert.Nil(t, generator)
	})

	t.Run("buf generator takes priority over protoc generators", func(t *testing.T) {
		tempDir := t.TempDir()
		bufGenYamlPath := filepath.Join(tempDir, "buf.gen.yaml")
		err := os.WriteFile(bufGenYamlPath, []byte("version: v1\n"), 0o644)
		require.NoError(t, err)

		protoFile := filepath.Join(tempDir, "test.proto")
		err = os.WriteFile(protoFile, []byte("syntax = \"proto3\";\n"), 0o644)
		require.NoError(t, err)

		generator := r.FindApplicableGenerator(tempDir)
		require.NotNil(t, generator)
		assert.Equal(t, SdkBuf, generator.SDK())
	})
}
