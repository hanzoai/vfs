# Security Policy

## Reporting a vulnerability

Email security@hanzo.ai with details. Encrypt with our PGP key (fingerprint TBD).

We respond within 48 hours. Critical issues receive same-day acknowledgment.

## Scope

This policy covers code in this repository. For the broader Hanzo platform threat model, see [hanzoai/HIPs](https://github.com/hanzoai/HIPs).

## Sandbox boundary

`vfs` encrypts every block client-side with `luxfi/age` (X25519, optionally hybrid PQ via ML-KEM-768) before any byte touches the configured backend, so the object store sees only ciphertext. Block integrity is enforced via blake3-256 content addressing — corruption or substitution by the backend is detected on read.

For runtime sandbox guarantees, see HIP-0105 (in-process extension runtimes).
