package relay

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
)

// Converts the user input name into the actual name
func queueName(name string) string {
	return "relay." + name
}

// Generates a channel name in the form of <host>.<rand>
// The random value is a hex encoding of 4 random bytes.
func channelName() (string, error) {
	// Get hostname
	host, err := os.Hostname()
	if err != nil {
		return "", fmt.Errorf("Failed to get hostname! Got: %s", err)
	}

	// Get random bytes
	bytes := make([]byte, 4)
	n, err := io.ReadFull(rand.Reader, bytes)
	if n != len(bytes) || err != nil {
		return "", fmt.Errorf("Failed to read random bytes! Got: %s", err)
	}

	// Convert to hex
	h := hex.EncodeToString(bytes)

	// Return the new name
	return host + "." + h, nil
}
