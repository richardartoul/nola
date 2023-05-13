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
	require.False(t, isServerIdBlacklistedActivationError(errors.New("random")))

	require.True(t, errors.Is(NewBlacklistedActivationError(errors.New("random"), []string{"abc"}), &BlacklistedActivationErr{}))
	require.True(t, errors.Is(NewBlacklistedActivationError(errors.New("random"), []string{"abc"}), BlacklistedActivationErr{}))
	require.True(t, isServerIdBlacklistedActivationError(NewBlacklistedActivationError(errors.New("random"), []string{"abc"})))
	require.True(t, isServerIdBlacklistedActivationError(fmt.Errorf("wrapped: %w", NewBlacklistedActivationError(errors.New("random"), []string{"abc"}))))

	require.Contains(t, NewBlacklistedActivationError(errors.New("random"), []string{"abc"}).(BlacklistedActivationErr).ServerIDs(), "abc")

	var httpErr HTTPError
	require.True(t, errors.As(
		NewBlacklistedActivationError(errors.New("random"), []string{"abc"}), &httpErr))
}
