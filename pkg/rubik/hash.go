package rubik

import "github.com/zeebo/blake3"

// contentID is the address a chunk is stored under: the hash of its plaintext, as
// vfs names a chunk. Two bodies with the same words in a different order are two
// addresses, which is why a rebuild costs bytes the store already holds.
func contentID(body []byte) [32]byte { return blake3.Sum256(body) }
