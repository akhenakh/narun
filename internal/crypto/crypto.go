package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
)

const (
	MasterKeySize = 32 // AES-256 requires a 32-byte key
	NonceSize     = 12 // Standard nonce size for AES-GCM
)

// DeriveKeyFromBase64 decodes a base64 master key and validates its size.
func DeriveKeyFromBase64(masterKeyBase64 string) ([]byte, error) {
	key, err := base64.StdEncoding.DecodeString(masterKeyBase64)
	if err != nil {
		return nil, fmt.Errorf("invalid base64 master key: %w", err)
	}
	if len(key) != MasterKeySize {
		return nil, fmt.Errorf("master key size mismatch: expected %d bytes, got %d", MasterKeySize, len(key))
	}
	return key, nil
}

// Encrypt encrypts plaintext using AES-256-GCM.
// It generates a random nonce and prepends it to the ciphertext.
// AAD (Additional Authenticated Data) is optional but recommended for integrity.
func Encrypt(masterKey []byte, plaintext []byte, aad []byte) (ciphertext []byte, nonce []byte, err error) {
	if len(masterKey) != MasterKeySize {
		return nil, nil, fmt.Errorf("invalid master key size: %d", len(masterKey))
	}

	block, err := aes.NewCipher(masterKey)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create AES cipher: %w", err)
	}

	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create GCM cipher: %w", err)
	}

	// Generate a unique nonce for each encryption
	nonce = make([]byte, NonceSize)
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, nil, fmt.Errorf("failed to generate nonce: %w", err)
	}

	// Encrypt the data
	ciphertext = aesgcm.Seal(nil, nonce, plaintext, aad) // Seal appends ciphertext to dst (nil here)

	return ciphertext, nonce, nil
}

// Decrypt decrypts ciphertext using AES-256-GCM.
// It expects the nonce used during encryption.
// AAD must match the AAD used during encryption.
func Decrypt(masterKey []byte, ciphertext []byte, nonce []byte, aad []byte) (plaintext []byte, err error) {
	if len(masterKey) != MasterKeySize {
		return nil, fmt.Errorf("invalid master key size: %d", len(masterKey))
	}
	if len(nonce) != NonceSize {
		return nil, fmt.Errorf("invalid nonce size: %d", len(nonce))
	}

	block, err := aes.NewCipher(masterKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create AES cipher: %w", err)
	}

	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM cipher: %w", err)
	}

	// Decrypt the data
	plaintext, err = aesgcm.Open(nil, nonce, ciphertext, aad)
	if err != nil {
		// This error can indicate incorrect key, nonce, ciphertext tampering, or incorrect AAD
		return nil, fmt.Errorf("failed to decrypt/authenticate data: %w", err)
	}

	return plaintext, nil
}
