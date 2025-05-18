package crypto

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"strings"
	"testing"
)

// Helper function to generate a random key of the correct size
func generateTestKey() ([]byte, error) {
	key := make([]byte, MasterKeySize)
	_, err := rand.Read(key)
	return key, err
}

// Helper function to generate a base64 encoded random key
func generateTestBase64Key() (string, []byte, error) {
	key, err := generateTestKey()
	if err != nil {
		return "", nil, err
	}
	b64Key := base64.StdEncoding.EncodeToString(key)
	return b64Key, key, nil
}

func TestDeriveKeyFromBase64(t *testing.T) {
	t.Run("ValidKey", func(t *testing.T) {
		b64Key, originalKey, err := generateTestBase64Key()
		if err != nil {
			t.Fatalf("Failed to generate test key: %v", err)
		}

		derivedKey, err := DeriveKeyFromBase64(b64Key)
		if err != nil {
			t.Fatalf("DeriveKeyFromBase64 failed with valid key: %v", err)
		}

		if !bytes.Equal(originalKey, derivedKey) {
			t.Errorf("Derived key does not match original key. Got %x, want %x", derivedKey, originalKey)
		}
	})

	t.Run("InvalidBase64", func(t *testing.T) {
		invalidB64 := "this is not base64!@#"
		_, err := DeriveKeyFromBase64(invalidB64)
		if err == nil {
			t.Fatal("DeriveKeyFromBase64 succeeded with invalid base64, expected error")
		}
		if !strings.Contains(err.Error(), "invalid base64 master key") {
			t.Errorf("Expected invalid base64 error, got: %v", err)
		}
	})

	t.Run("IncorrectKeySizeTooSmall", func(t *testing.T) {
		smallKey := make([]byte, MasterKeySize-1)
		_, _ = rand.Read(smallKey)
		b64SmallKey := base64.StdEncoding.EncodeToString(smallKey)

		_, err := DeriveKeyFromBase64(b64SmallKey)
		if err == nil {
			t.Fatal("DeriveKeyFromBase64 succeeded with small key, expected error")
		}
		if !strings.Contains(err.Error(), "master key size mismatch") {
			t.Errorf("Expected key size mismatch error, got: %v", err)
		}
	})

	t.Run("IncorrectKeySizeTooLarge", func(t *testing.T) {
		largeKey := make([]byte, MasterKeySize+1)
		_, _ = rand.Read(largeKey)
		b64LargeKey := base64.StdEncoding.EncodeToString(largeKey)

		_, err := DeriveKeyFromBase64(b64LargeKey)
		if err == nil {
			t.Fatal("DeriveKeyFromBase64 succeeded with large key, expected error")
		}
		if !strings.Contains(err.Error(), "master key size mismatch") {
			t.Errorf("Expected key size mismatch error, got: %v", err)
		}
	})
}

func TestEncryptDecrypt(t *testing.T) {
	masterKey, err := generateTestKey()
	if err != nil {
		t.Fatalf("Failed to generate master key: %v", err)
	}

	plaintext := []byte("This is a secret message.")
	aad := []byte("Additional authenticated data") // Test with AAD
	nilAad := []byte(nil)                          // Test without AAD

	// Test Success with AAD
	t.Run("SuccessWithAAD", func(t *testing.T) {
		ciphertext, nonce, err := Encrypt(masterKey, plaintext, aad)
		if err != nil {
			t.Fatalf("Encryption failed: %v", err)
		}
		if len(nonce) != NonceSize {
			t.Fatalf("Incorrect nonce size: got %d, want %d", len(nonce), NonceSize)
		}
		if len(ciphertext) == 0 {
			t.Fatal("Ciphertext is empty")
		}

		decrypted, err := Decrypt(masterKey, ciphertext, nonce, aad)
		if err != nil {
			t.Fatalf("Decryption failed: %v", err)
		}

		if !bytes.Equal(plaintext, decrypted) {
			t.Errorf("Decrypted text does not match original. Got %q, want %q", decrypted, plaintext)
		}
	})

	// Test Success without AAD
	t.Run("SuccessWithoutAAD", func(t *testing.T) {
		ciphertext, nonce, err := Encrypt(masterKey, plaintext, nilAad) // Or just pass nil
		if err != nil {
			t.Fatalf("Encryption failed: %v", err)
		}

		decrypted, err := Decrypt(masterKey, ciphertext, nonce, nilAad) // Or just pass nil
		if err != nil {
			t.Fatalf("Decryption failed: %v", err)
		}

		if !bytes.Equal(plaintext, decrypted) {
			t.Errorf("Decrypted text does not match original. Got %q, want %q", decrypted, plaintext)
		}
	})

	// Test Decryption Failure - Wrong Key
	t.Run("FailWrongKey", func(t *testing.T) {
		ciphertext, nonce, err := Encrypt(masterKey, plaintext, aad)
		if err != nil {
			t.Fatalf("Encryption failed: %v", err)
		}

		wrongKey, _ := generateTestKey()
		_, err = Decrypt(wrongKey, ciphertext, nonce, aad)
		if err == nil {
			t.Fatal("Decryption succeeded with wrong key, expected error")
		}
		if !strings.Contains(err.Error(), "failed to decrypt/authenticate data") {
			t.Errorf("Expected decryption failure error, got: %v", err)
		}
	})

	// Test Decryption Failure - Tampered Ciphertext
	t.Run("FailTamperedCiphertext", func(t *testing.T) {
		ciphertext, nonce, err := Encrypt(masterKey, plaintext, aad)
		if err != nil {
			t.Fatalf("Encryption failed: %v", err)
		}

		// Tamper with the ciphertext (flip a bit)
		if len(ciphertext) > 0 {
			ciphertext[len(ciphertext)-1] ^= 0x01 // Flip last bit
		} else {
			t.Log("Skipping tamper test on empty ciphertext") // Should not happen with GCM
			return
		}

		_, err = Decrypt(masterKey, ciphertext, nonce, aad)
		if err == nil {
			t.Fatal("Decryption succeeded with tampered ciphertext, expected error")
		}
		if !strings.Contains(err.Error(), "failed to decrypt/authenticate data") {
			t.Errorf("Expected decryption failure error, got: %v", err)
		}
	})

	// Test Decryption Failure - Wrong Nonce
	t.Run("FailWrongNonce", func(t *testing.T) {
		ciphertext, _, err := Encrypt(masterKey, plaintext, aad) // Ignore original nonce
		if err != nil {
			t.Fatalf("Encryption failed: %v", err)
		}

		wrongNonce := make([]byte, NonceSize)
		_, _ = rand.Read(wrongNonce) // Generate a different nonce

		_, err = Decrypt(masterKey, ciphertext, wrongNonce, aad)
		if err == nil {
			t.Fatal("Decryption succeeded with wrong nonce, expected error")
		}
		if !strings.Contains(err.Error(), "failed to decrypt/authenticate data") {
			t.Errorf("Expected decryption failure error, got: %v", err)
		}
	})

	// Test Decryption Failure - Wrong AAD
	t.Run("FailWrongAAD", func(t *testing.T) {
		ciphertext, nonce, err := Encrypt(masterKey, plaintext, aad)
		if err != nil {
			t.Fatalf("Encryption failed: %v", err)
		}

		wrongAad := []byte("Different AAD")
		_, err = Decrypt(masterKey, ciphertext, nonce, wrongAad)
		if err == nil {
			t.Fatal("Decryption succeeded with wrong AAD, expected error")
		}
		if !strings.Contains(err.Error(), "failed to decrypt/authenticate data") {
			t.Errorf("Expected decryption failure error, got: %v", err)
		}
	})

	// Test Decryption Failure - AAD Mismatch (encrypted with AAD, decrypted without)
	t.Run("FailAADMismatchEncryptWithDecryptWithout", func(t *testing.T) {
		ciphertext, nonce, err := Encrypt(masterKey, plaintext, aad)
		if err != nil {
			t.Fatalf("Encryption failed: %v", err)
		}

		_, err = Decrypt(masterKey, ciphertext, nonce, nilAad) // Decrypt without AAD
		if err == nil {
			t.Fatal("Decryption succeeded with AAD mismatch, expected error")
		}
		if !strings.Contains(err.Error(), "failed to decrypt/authenticate data") {
			t.Errorf("Expected decryption failure error, got: %v", err)
		}
	})

	// Test Decryption Failure - AAD Mismatch (encrypted without AAD, decrypted with)
	t.Run("FailAADMismatchEncryptWithoutDecryptWith", func(t *testing.T) {
		ciphertext, nonce, err := Encrypt(masterKey, plaintext, nilAad) // Encrypt without AAD
		if err != nil {
			t.Fatalf("Encryption failed: %v", err)
		}

		_, err = Decrypt(masterKey, ciphertext, nonce, aad) // Decrypt with AAD
		if err == nil {
			t.Fatal("Decryption succeeded with AAD mismatch, expected error")
		}
		if !strings.Contains(err.Error(), "failed to decrypt/authenticate data") {
			t.Errorf("Expected decryption failure error, got: %v", err)
		}
	})
}

func TestEncrypt_InvalidKeySize(t *testing.T) {
	invalidKey := make([]byte, MasterKeySize-1)
	plaintext := []byte("test")
	aad := []byte("test_aad")

	_, _, err := Encrypt(invalidKey, plaintext, aad)
	if err == nil {
		t.Fatal("Encrypt succeeded with invalid key size, expected error")
	}
	if !strings.Contains(err.Error(), "invalid master key size") {
		t.Errorf("Expected invalid key size error, got: %v", err)
	}
}

func TestDecrypt_InvalidKeySize(t *testing.T) {
	invalidKey := make([]byte, MasterKeySize-1)
	ciphertext := []byte("dummy")
	nonce := make([]byte, NonceSize)
	aad := []byte("test_aad")

	_, err := Decrypt(invalidKey, ciphertext, nonce, aad)
	if err == nil {
		t.Fatal("Decrypt succeeded with invalid key size, expected error")
	}
	if !strings.Contains(err.Error(), "invalid master key size") {
		t.Errorf("Expected invalid key size error, got: %v", err)
	}
}

func TestDecrypt_InvalidNonceSize(t *testing.T) {
	masterKey, err := generateTestKey()
	if err != nil {
		t.Fatalf("Failed to generate master key: %v", err)
	}
	invalidNonce := make([]byte, NonceSize-1)
	ciphertext := []byte("dummy")
	aad := []byte("test_aad")

	_, err = Decrypt(masterKey, ciphertext, invalidNonce, aad)
	if err == nil {
		t.Fatal("Decrypt succeeded with invalid nonce size, expected error")
	}
	if !strings.Contains(err.Error(), "invalid nonce size") {
		t.Errorf("Expected invalid nonce size error, got: %v", err)
	}
}

// Optional: Test with empty plaintext
func TestEncryptDecrypt_EmptyPlaintext(t *testing.T) {
	masterKey, err := generateTestKey()
	if err != nil {
		t.Fatalf("Failed to generate master key: %v", err)
	}

	plaintext := []byte{} // Empty plaintext
	aad := []byte("empty_plaintext_aad")

	ciphertext, nonce, err := Encrypt(masterKey, plaintext, aad)
	if err != nil {
		t.Fatalf("Encryption failed for empty plaintext: %v", err)
	}
	if len(nonce) != NonceSize {
		t.Fatalf("Incorrect nonce size: got %d, want %d", len(nonce), NonceSize)
	}
	// Ciphertext for empty plaintext is just the GCM tag
	if len(ciphertext) == 0 {
		t.Fatal("Ciphertext is empty (expected GCM tag)")
	}

	decrypted, err := Decrypt(masterKey, ciphertext, nonce, aad)
	if err != nil {
		t.Fatalf("Decryption failed for empty plaintext: %v", err)
	}

	// Important: Decrypting empty plaintext should result in an empty slice (`[]byte{}`), NOT a nil slice (`nil`).
	// This maintains consistency, as the cryptographic operation on zero bytes results in zero bytes,
	// represented by an empty slice. While often behaving similarly, `nil` and `[]byte{}` are distinct in Go.
	// Returning a non-nil empty slice avoids potential nil-pointer issues if the caller isn't careful.
	if decrypted == nil {
		t.Errorf("Decrypted text is nil, want empty slice []byte{}")
	}
	if !bytes.Equal(plaintext, decrypted) {
		t.Errorf("Decrypted text does not match original empty slice. Got %q (%d bytes), want %q (%d bytes)", decrypted, len(decrypted), plaintext, len(plaintext))
	}

}
