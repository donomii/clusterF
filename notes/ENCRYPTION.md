# Repository encryption modes

Encryption is disabled by default. Supplying `--encryption-key` when creating an empty repository enables encryption for stored file metadata and contents. The same password is required when reopening that repository. Each node has its own encryption settings and may use a different password; replication exchanges decrypted file data through the existing HTTP protocol. Neither storage format provides transport encryption.

`--encryption-mode` controls format selection for single nodes and simulations:

| Mode | Behavior |
| --- | --- |
| `auto` (default) | Detects the saved format and preserves it. Creates authenticated encryption for a new repository when a password is supplied; otherwise creates an unencrypted repository. |
| `legacy` | Reads and writes the original repeating-key format. Requires a password. Can create a legacy repository or open an existing legacy repository. |
| `authenticated` | Reads and writes AES-256-GCM streams. Requires a password. Can create an authenticated repository or open an existing authenticated repository. |

Existing legacy repositories open with their original password and the default `auto` mode. Opening or reading them does not rewrite their settings or encrypted data, and subsequent writes remain compatible with the original format. Explicit mode selections must match existing repository settings. No mode performs an implicit conversion.

## Legacy format

Legacy settings contain `encrypted_test_phrase`: hexadecimal bytes produced by XORing `clusterF-encryption-test` with the repeating password bytes. File metadata and contents use the same operation independently, each starting at password offset zero. Streaming reads and writes preserve that offset across chunks. This format is retained for compatibility; it does not authenticate data or provide the protection of the authenticated format.

Missing passwords, failed password verification, malformed verification values, and settings containing both legacy and authenticated markers are rejected before repository files are changed. The original legacy verification phrase checks at most the first 24 password bytes; legacy mode retains that limitation for compatibility.

## Authenticated format

The node's `settings.json` contains an `encryption` object with these fields:

| Field | Meaning |
| --- | --- |
| `format` | `clusterF-aes256gcm-pbkdf2-sha256-v1` |
| `salt` | Random 32-byte salt, represented as base64 in JSON |
| `verification` | Authenticated encryption of the format identifier, represented as base64 in JSON |

The password and salt derive a 32-byte key using PBKDF2 with SHA-256 and 600,000 iterations. The verification value uses the context `repository-key-verification`. Invalid settings and an absent or incorrect password prevent storage from opening.

### Stored streams

An encrypted value begins with the bytes `CLUSTERF-AEAD-1\n`, followed by a random 32-byte stream identifier. Each subsequent frame contains a four-byte unsigned big-endian length followed by an AES-256-GCM ciphertext with a random 12-byte nonce prepended and a 16-byte authentication tag appended. A frame encrypts at most 65,536 plaintext bytes.

The additional authenticated data is the context string, a zero byte, the stream identifier, and an eight-byte unsigned big-endian frame number starting at zero. File contexts are `metadata:` followed by the complete stored path, or `content:` followed by that path. This binds each frame to its file, purpose, stream, and position.

Every stream ends with an authenticated frame containing zero plaintext bytes. Missing final frames, altered ciphertext, out-of-order frames, frames copied from another stream or path, and bytes after the final frame are rejected. Authentication errors are returned to the reader. Metadata is fully decoded before use; file contents can be read a frame at a time.

## Compatibility

Unencrypted repositories remain unencrypted. Enabling encryption on an existing unencrypted repository is rejected because its contents require an explicit migration.

Legacy and authenticated repositories retain their saved formats when reopened. Choosing the other mode is rejected before writes; changing formats requires an explicit migration, and a migration utility is not included in this change.

The raw-file store serializes publication of metadata/content pairs and opens read snapshots under the same lock. If metadata publication fails, the previous content is restored; a failed restoration reports the retained backup location. This prevents mixed versions during concurrent operations and recoverable write errors. It is not a crash-recovery journal, and does not guarantee recovery from interruption between the two filesystem renames.
