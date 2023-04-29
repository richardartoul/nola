package virtual

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBlacklistedActivationError(t *testing.T) {
	require.False(t, errors.Is(errors.New("random"), &BlacklistedActivationErr{}))
	require.False(t, errors.Is(errors.New("random"), BlacklistedActivationErr{}))
	require.False(t, IsBlacklistedActivationError(errors.New("random")))

	require.True(t, errors.Is(NewBlacklistedActivationError(errors.New("random")), &BlacklistedActivationErr{}))
	require.True(t, errors.Is(NewBlacklistedActivationError(errors.New("random")), BlacklistedActivationErr{}))
	require.True(t, IsBlacklistedActivationError(NewBlacklistedActivationError(errors.New("random"))))
	require.True(t, IsBlacklistedActivationError(fmt.Errorf("wrapped: %w", NewBlacklistedActivationError(errors.New("random")))))
}
