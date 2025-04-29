package main

import (
	"debug/elf"
	"debug/macho"
	"debug/pe"
	"fmt"
	"io"
	"os"
)

// detectBinaryPlatform attempts to determine OS and Arch from executable headers.
// Currently supports ELF (Linux), Mach-O (macOS), and PE (Windows).
func detectBinaryPlatform(filePath string) (osName string, archName string, err error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", "", fmt.Errorf("failed to open binary %s: %w", filePath, err)
	}
	defer file.Close()

	// Read the first few bytes to identify the format
	magic := make([]byte, 4)
	_, err = io.ReadAtLeast(file, magic, 4)
	if err != nil {
		return "", "", fmt.Errorf("failed to read magic bytes from %s: %w", filePath, err)
	}

	// Reset file offset for subsequent parsers
	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		return "", "", fmt.Errorf("failed to seek back to start of %s: %w", filePath, err)
	}

	// Check for ELF (Linux/Unix)
	if string(magic) == elf.ELFMAG {
		elfFile, err := elf.NewFile(file)
		if err != nil {
			return "", "", fmt.Errorf("failed to parse ELF header for %s: %w", filePath, err)
		}
		defer elfFile.Close() // Close ELF file handle

		// Determine OS - ELFOSABI_LINUX or ELFOSABI_NONE often mean Linux for Go builds
		switch elfFile.OSABI {
		case elf.ELFOSABI_LINUX, elf.ELFOSABI_NONE:
			osName = "linux"
		case elf.ELFOSABI_FREEBSD:
			osName = "freebsd"
		case elf.ELFOSABI_OPENBSD:
			osName = "openbsd"
		case elf.ELFOSABI_NETBSD:
			osName = "netbsd"
		case elf.ELFOSABI_SOLARIS:
			osName = "solaris"
		default:
			return "", "", fmt.Errorf("unsupported ELF OSABI '%s' in %s", elfFile.OSABI, filePath)
		}

		// Determine Arch
		switch elfFile.Machine {
		case elf.EM_X86_64:
			archName = "amd64"
		case elf.EM_AARCH64:
			archName = "arm64"
		case elf.EM_RISCV:
			archName = "riscv64"
		case elf.EM_386:
			archName = "386"
		case elf.EM_ARM:
			archName = "arm" // Note: Need to check ELF flags for ARM version (e.g., armv7) if precision needed
		// Add other EM_ mappings if needed
		default:
			return "", "", fmt.Errorf("unsupported ELF machine type '%s' in %s", elfFile.Machine, filePath)
		}
		return osName, archName, nil
	}

	// Check for Mach-O (macOS) - Magic numbers vary (32/64 bit, endianness)
	// macho.NewFile handles these variations.
	_, err = file.Seek(0, io.SeekStart) // Seek back again
	if err != nil {
		return "", "", fmt.Errorf("failed to seek for Mach-O check %s: %w", filePath, err)
	}
	machoFile, err := macho.NewFile(file)
	if err == nil { // If it parses as Mach-O
		defer machoFile.Close()
		osName = "darwin"
		switch machoFile.Cpu {
		case macho.CpuAmd64:
			archName = "amd64"
		case macho.CpuArm64:
			archName = "arm64"
		// Add other CPU types if needed
		default:
			return "", "", fmt.Errorf("unsupported Mach-O CPU type '%s' in %s", machoFile.Cpu, filePath)
		}
		return osName, archName, nil
	}
	// Don't error yet if it's not Mach-O, try PE

	// Check for PE (Windows)
	_, err = file.Seek(0, io.SeekStart) // Seek back again
	if err != nil {
		return "", "", fmt.Errorf("failed to seek for PE check %s: %w", filePath, err)
	}
	peFile, err := pe.NewFile(file)
	if err == nil { // If it parses as PE
		defer peFile.Close()
		osName = "windows"
		switch peFile.Machine {
		case pe.IMAGE_FILE_MACHINE_AMD64:
			archName = "amd64"
		case pe.IMAGE_FILE_MACHINE_ARM64:
			archName = "arm64"
		case pe.IMAGE_FILE_MACHINE_I386:
			archName = "386"

		// Add other machine types if needed
		default:
			return "", "", fmt.Errorf("unsupported PE machine type '%#x' in %s", peFile.Machine, filePath)
		}
		return osName, archName, nil
	}

	return "", "", fmt.Errorf("unsupported or unrecognized executable format for %s", filePath)
}
