// narun/internal/noderunner/process_test.go
package noderunner

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestVerifyLocalFileHash covers various scenarios for the hash verification function.
func TestVerifyLocalFileHash(t *testing.T) {
	contentValid := "hello world narun runner"
	// Calculate expected digest for contentValid:
	// echo -n "hello world narun runner" | sha256sum -> e0517cc2d4f79798fdea30b2580139d51f8296254a87034a43af6f6687b4b063
	// go run convert_hash.go e0517cc2d4f79798fdea30b2580139d51f8296254a87034a43af6f6687b4b063 -> 4FF8wtT3l5j96jCyWAE51R-CliVKhwNKQ69vZoe0sGM
	digestValid := "SHA-256=4FF8wtT3l5j96jCyWAE51R-CliVKhwNKQ69vZoe0sGM"

	digestMismatch := "SHA-256=NotTheRightBase64HashValueForContentMismatch" // Valid format, wrong value

	contentEmpty := ""
	// Calculate expected digest for empty content:
	// echo -n "" | sha256sum -> e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
	// go run convert_hash.go e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855 -> 47DEQpj8HBSa-_TImW-5JCeuQeRkm5NMpJWZG3hSuFU
	digestEmpty := "SHA-256=47DEQpj8HBSa-_TImW-5JCeuQeRkm5NMpJWZG3hSuFU"
	//  End Test Data Setup

	//  Test Cases Table
	tests := []struct {
		name           string  // Name of the test case
		fileContent    *string // Pointer to content, nil means don't create file
		expectedDigest string  // Digest string passed to the function
		wantMatch      bool    // Expected boolean result (if no error)
		wantErr        bool    // Whether an error is expected
		errorContains  string  // Substring expected in error message (if wantErr is true)
	}{
		{
			name:           "Valid File and Matching Hash",
			fileContent:    &contentValid,
			expectedDigest: digestValid,
			wantMatch:      true,
			wantErr:        false,
		},
		{
			name:           "Valid File but Mismatching Hash",
			fileContent:    &contentValid,
			expectedDigest: digestMismatch, // Provide a different hash
			wantMatch:      false,          // Expect mismatch
			wantErr:        false,
		},
		{
			name:           "Empty File and Matching Hash",
			fileContent:    &contentEmpty,
			expectedDigest: digestEmpty,
			wantMatch:      true,
			wantErr:        false,
		},
		{
			name:           "Empty File and Mismatching Hash",
			fileContent:    &contentEmpty,
			expectedDigest: digestValid, // Hash for non-empty content
			wantMatch:      false,
			wantErr:        false,
		},
		{
			name:           "File Not Found",
			fileContent:    nil, // Don't create the file
			expectedDigest: digestValid,
			wantMatch:      false,
			wantErr:        true,
			errorContains:  "failed to open file for hashing", // Expect file open error
		},
		{
			name:           "Invalid Digest Format (No Prefix)",
			fileContent:    &contentValid,
			expectedDigest: "JustTheBase64Hash", // Missing "SHA-256="
			wantMatch:      false,
			wantErr:        true,
			errorContains:  "unsupported digest format",
		},
		{
			name:           "Invalid Digest Format (Wrong Prefix)",
			fileContent:    &contentValid,
			expectedDigest: "MD5=SomeHash", // Wrong prefix
			wantMatch:      false,
			wantErr:        true,
			errorContains:  "unsupported digest format",
		},
		{
			name:           "Invalid Digest Format (Empty Hash Value)",
			fileContent:    &contentValid,
			expectedDigest: "SHA-256=", // Empty hash after prefix
			wantMatch:      false,
			wantErr:        true,
			errorContains:  "expected digest value is empty",
		},
		{
			name:           "Invalid Digest Format (Prefix Only)",
			fileContent:    &contentValid,
			expectedDigest: "SHA-256", // Prefix without '='
			wantMatch:      false,
			wantErr:        true,
			errorContains:  "unsupported digest format",
		},
	}
	// --- End Test Cases Table ---

	// --- Run Tests ---
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create temp directory for the test file
			tempDir := t.TempDir()
			filePath := filepath.Join(tempDir, "testfile.bin")

			// Create the file if content is provided
			if tt.fileContent != nil {
				err := os.WriteFile(filePath, []byte(*tt.fileContent), 0644)
				if err != nil {
					t.Fatalf("Failed to create temporary file %s: %v", filePath, err)
				}
				defer os.Remove(filePath) // Clean up even though TempDir does too
			} else {
				// Ensure the file doesn't exist for the "File Not Found" case
				_ = os.Remove(filePath) // Ignore error if it already didn't exist
			}

			// Call the function under test
			gotMatch, err := verifyLocalFileHash(filePath, tt.expectedDigest)

			// Assert error expectation
			if (err != nil) != tt.wantErr {
				t.Errorf("verifyLocalFileHash() error = %v, wantErr %v", err, tt.wantErr)
				return // Stop further checks if error expectation is wrong
			}

			// Assert error content if an error was expected
			if tt.wantErr && tt.errorContains != "" {
				if err == nil || !strings.Contains(err.Error(), tt.errorContains) {
					t.Errorf("verifyLocalFileHash() error = %v, want error containing %q", err, tt.errorContains)
				}
				// Don't check match result if an error occurred
				return
			}

			// Assert match result if no error was expected
			if !tt.wantErr && gotMatch != tt.wantMatch {
				t.Errorf("verifyLocalFileHash() gotMatch = %v, want %v", gotMatch, tt.wantMatch)
			}
		})
	}
}

// Helper function for converting hex to base64 if needed during test setup
// (You already have this externally, but might include it here if desired)
/*
func hexToBase64(hexHash string) (string, error) {
	hashBytes, err := hex.DecodeString(hexHash)
	if err != nil {
		return "", fmt.Errorf("Error decoding hex string: %w", err)
	}
	base64Hash := base64.URLEncoding.WithPadding(base64.NoPadding).EncodeToString(hashBytes)
	return base64Hash, nil
}
*/
