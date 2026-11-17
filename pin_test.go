package vfs

// Layout bytes that docs/SPECIFICATION.md fixes and that spec_test.go does not pin: each
// test below passes on the tree as it is and fails when the byte moves (the mutations are
// in the report). A fixture produced by the current luxfi/age holds the Cipher's format.

import (
	"bytes"
	"context"
	"encoding/hex"
	"regexp"
	"testing"

	"github.com/luxfi/age"
	"github.com/zeebo/blake3"

	"github.com/hanzoai/vfs/internal/storetest"
)

func TestObjectKeysAreTheSpecShapes(t *testing.T) {
	ctx := context.Background()
	be := storetest.NewMem()
	k, _ := age.GenerateHybridIdentity()
	c := Hybrid(k)
	if err := Create(ctx, Config{Backend: be, Cipher: c}); err != nil {
		t.Fatal(err)
	}
	v, err := Attach(ctx, Config{Backend: be, Cipher: c, Round: 1, Journal: storetest.Journals(), Capacity: 16 << 20, Disk: storetest.Disk})
	if err != nil {
		t.Fatal(err)
	}
	in, _ := v.Create(ctx, RootInode, "big", 0o644)
	f, _ := v.Open(ctx, in.ID)
	data := make([]byte, 3<<20)
	for i := range data {
		data[i] = byte(i*7 + i>>9)
	}
	if _, err := f.WriteAt(ctx, data, 0); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if err := v.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	meta := regexp.MustCompile(`^meta/([0-9a-f]{2})/([0-9a-f]{64})$`)
	pack := regexp.MustCompile(`^pack/([0-9a-f]{2})/([0-9a-f]{32})$`)
	var shards, packs int
	for key, obj := range be.Dump() {
		switch {
		case key == "head":
		case meta.MatchString(key):
			m := meta.FindStringSubmatch(key)
			sum := blake3.Sum256(obj)
			if m[2] != hex.EncodeToString(sum[:]) || m[1] != m[2][:2] {
				t.Errorf("%s: not meta/<first byte>/<BLAKE3 of the sealed bytes>", key)
			}
			shards++
		case pack.MatchString(key):
			if m := pack.FindStringSubmatch(key); m[1] != m[2][:2] {
				t.Errorf("%s: fan-out is not the first byte of the id", key)
			}
			packs++
		default:
			t.Errorf("key %q is not a key of SPEC 3", key)
		}
	}
	if shards == 0 || packs == 0 {
		t.Fatalf("shards=%d packs=%d", shards, packs)
	}
}

// 5.2: the token is 16 bytes, so the sealed Head begins at byte 30 and opens there.
func TestFenceTokenIsSixteenBytes(t *testing.T) {
	be := storetest.NewMem()
	k, _ := age.GenerateHybridIdentity()
	c := Hybrid(k)
	if err := Create(context.Background(), Config{Backend: be, Cipher: c}); err != nil {
		t.Fatal(err)
	}
	raw := be.Dump()["head"]
	if _, err := unseal(c, headAAD(be.String()), raw[30:]); err != nil {
		t.Fatalf("the sealed Head does not begin at byte 30 of the record: %v", err)
	}
}

// SPEC 7, 6.2, 9: the sizes the text states.
func TestTheSizesOfTheText(t *testing.T) {
	for name, c := range map[string][2]int{
		"inline file 16 KiB": {small, 16 << 10}, "min chunk 64 KiB": {minChunk, 64 << 10}, "max chunk 4 MiB": {maxChunk, 4 << 20},
		"avgBits 19": {avgBits, 19}, "node target 64 KiB": {nodeTarget, 64 << 10}, "node cap 512 KiB": {nodeMax, 512 << 10}, "head bound 64 KiB": {maxHead, 64 << 10},
	} {
		if c[0] != c[1] {
			t.Errorf("%s: the code has %d", name, c[0])
		}
	}
	if uint64(MaxSize) != 1<<62 {
		t.Errorf("file size limit 2^62: the code has %d", uint64(MaxSize))
	}
}

const goldenIdentity = "AGE-SECRET-KEY-PQ-1NQY3A58D94LE0LYG47MA22G7TZTT8FMXP9776W4ZY3NUD7G6JADQDECSEM"

// goldenFrame is an object that luxfi/age sealed to goldenIdentity with the context of a Head
// of the place s3://golden/place and the payload "golden payload of the layout".
const goldenFrame = "10363763643437376433366133663331376167652d656e6372797074696f6e2e6f72672f76310a2d3e206d6c6b656d37363878323535313920505433343973643354766c484f6733534a2b396978347a6749584b4f3662567262685935794d5747666257637a6f526f746a506f78334334644f6f357a744b6b36626e506d334a50486346414a2f556a7568326f576a673463676675755157746f7a5438383530484458786f4d59566d574c616d476e42646b6d75494379562b7434666d6165334461457a55345933395a6e715748536f4c334f3373756f5a4f62466a555141675a58624f7936733035792b644d4f324153694a6469555051316872733149725261336b5333464d5076636934486f4f2b4f6936656d4b7544745932575938637a6d5351306d63574d4e4f2b4d72306644322f656f47766a62516b78426c34444179356b332f716c773654777878534c436e437456756650636e303364516f4e744c4443704f34436674476e56753252747a376f61343345554b424d78747968686f6a312f37536e697378307945736e4231616638595564337a78626f544a424e55654e38353563614b456b4a58697466497157695a456c4a3772525875487977576759556b323442794c384547335765304e6d62794d6273654c4357596c6f32313947537a7633584c6f727561352f2f487a55376464353165574a68634e553875545a5571345a447044583075507574325161523462346e41384869612b416273784458457a4c774861497441356f794742557261555474582f6a6d6449576b366c5077466752336e33733975796346676b525953655655303874586157336752455a7538473664473141655a3050626959626131367648337163674e3057716933665a4b45744b6a74646a734747775056514861424e3968625035727041747474767174546c71322b76665063667a4d776957594e672b7a69772f346a5075645345466d4d543477567444495155467a5552484a6c7a3075366439542b77652b6c786c2f30484d5159553453706f53434a2f52516477426d36786d726e44342b7175615262445947543445754e4e4666727156395554503865336442447736576779736c665a554e6a682f666e7a5a567076794e4249616173536e4c6170744d72324f61304c57333752704235736b63696a57355453555070776f514547736c7a63744c7645536d4a733749536a64726f7a7033524459764f61514d5a696f69436e53397365436251686d6c313237716367367a4e2f486a626c774d3349725a316358726b446c3333624e5773586330756d6e6e69466277386f43496971493647766b644e4263473055374f6a717a2b75486d667a333464686f5a346c6d43766a54776363316277384f6a494538366f6c736a6b59572b5a5953754f58452b57306232536b4e4b4e454d774434565a50344146384e6c78474f69522b5535346e527a51424e61345548796e74676c6a2f724956304542556450774e4f61596748673252717a65517868586976306f5957795858584a4d5741566a68674e5a79794a6e723549656535346c46624e3975666250316341393033793842543279542b66652b71436578474779497848785675652f356f4d564a54504a584d5a317252444441656c342b2f6434794e75795536526e376c776f5177395379435a66774f526f62544f564b3972536a4d2f612f6374543650766d3858585135664d34506243424e2b3236474d684356696f4153364534596b75417644664265723134582f63334649354c50713562342f6a6847662b456e4d52674331776d342f4f38364336526d75516130424733523872526e2f742f627036613843314e71396c75467748775a75446a67756545624c412f5a6748366f354a4e4d6634753331446a2f643365704b4a7347414e62504b455a7461377244416643677950594b576c54386c57442f4c2b514e3074793078784850714c702b467266414f59655854496b6a317158636b635649526a62725168566d4f716132444d314c34323679746451576e484d69613253754637376956374876353572676d41553246346454422f2f4262443336435330444a58507466523464677756456f576637774c6b4a495a36504e3155623648345573647154694a510a4e56414d54336278546a6249654c3837615944454869706b2b754a30516732374c6f424a744d6f3239316b0a2d2d2d206e322f57694856674e576d652b6e39724848346c796153676f684c4e616b746b71462b76774d72374c4d670a87931e016beb24965c11e4986a3d8f23d29791a197f52252d97aae964779e4e825ac38d2104742e2e1b033594426ec6eff4298d679c739e7e49bcac3be4f9ff036efdabc799ae1ea30aedd541197e418578460d4fa5194b9babb5cecf001b0d585733c25064942493c8d0b56e00fcf999838f08c6f6a36463e0ace64"

// 4.1: the default Cipher opens what luxfi/age at the time of this test sealed. A change of the
// library that alters its file format, its recipient or the identity string fails here, not in
// every Volume. This is the direction of the open only: a build of the past that opens what a
// later library seals is checked by the test below, on the structure that a reader depends on.
func TestTheCipherOpensAGoldenObject(t *testing.T) {
	k, err := age.ParseHybridIdentity(goldenIdentity)
	if err != nil {
		t.Fatal(err)
	}
	frame, _ := hex.DecodeString(goldenFrame)
	got, err := unseal(Hybrid(k), bind("head", []byte("s3://golden/place")), frame)
	if err != nil {
		t.Fatalf("the golden object does not open: %v", err)
	}
	if !bytes.Equal(got, []byte("golden payload of the layout")) {
		t.Fatalf("golden payload %q", got)
	}
}

// 4.1: what the default Cipher seals now has the structure of the golden object, up to the bytes
// that are random: the length and the id of the key, the version line of the age file and the
// type of its recipient stanza. A reader of the recorded build parses exactly that first, so a
// library that changed it would write objects that the recorded build cannot open, and the test
// of the open direction would not show it.
func TestTheCipherSealsTheStructureOfTheGoldenObject(t *testing.T) {
	k, err := age.ParseHybridIdentity(goldenIdentity)
	if err != nil {
		t.Fatal(err)
	}
	golden, _ := hex.DecodeString(goldenFrame)
	sealed, err := seal(Hybrid(k), bind("head", []byte("s3://golden/place")), []byte("a payload of the layout"))
	if err != nil {
		t.Fatal(err)
	}
	const stanza = "age-encryption.org/v1\n-> mlkem768x25519 "
	n := 1 + int(golden[0])
	at := bytes.Index(golden, []byte(stanza))
	if at != n || len(sealed) < n+len(stanza) {
		t.Fatalf("the golden frame has its age file at %d, not after the key id (%d); sealed %d bytes", at, n, len(sealed))
	}
	if want := golden[:n+len(stanza)]; !bytes.Equal(sealed[:n+len(stanza)], want) {
		t.Fatalf("a sealed frame starts %q, the golden one %q", sealed[:n+len(stanza)], want)
	}
	if got, err := unseal(Hybrid(k), bind("head", []byte("s3://golden/place")), sealed); err != nil || string(got) != "a payload of the layout" {
		t.Fatalf("what this build seals does not open: %q %v", got, err)
	}
}
