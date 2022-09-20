package bigmap

import (
	"fmt"
	"testing"

	"github.com/dgraph-io/badger"
)

func TestSet(t *testing.T) {
	Init("./data")
	var tests = []struct {
		key, val string
	}{
		{"t1", "v1"},
		{"t2", "v2"},
		{"t3", "v3"},
	}
	for _, tt := range tests {
		testname := fmt.Sprintf("put %s,%s", tt.key, tt.val)
		t.Run(testname, func(t *testing.T) {
			Set(tt.key, tt.val)
			ans, err := Query(tt.key)
			if err != nil {
				t.Fail()
			}
			if ans != tt.val {
				t.Errorf("return:%s, want %s", ans, tt.val)
			}
		})
	}
	defer db.Close()

	//Query(db, k)
	//Seq(db)
}

func TestDelete(t *testing.T) {
	Init("./data")
	key := "test_d1"
	value := "value1"
	Set(key, value)
	res, _ := Query(key)
	fmt.Printf("value: %s\n", res)
	if res != value {
		t.Errorf("return:%s , want: %s\n", res, value)
	}
	Delete(key)
	var err error

	res, err = Query(key)
	if err != badger.ErrKeyNotFound {
		t.Errorf("return:%s , want: %s\n", res, badger.ErrKeyNotFound)
	}
	defer db.Close()

}

func TestSetWithTTL(t *testing.T) {
	Init("./data")
	defer db.Close()
	key := "aa"
	SetWithTTL(key, "aavalue", 3)
	for i := 0; i < 5; i++ {
		fmt.Printf("after %v second...\n", i)
		// time.Sleep(time.Second)
		err := view(db, []byte(key))
		if i < 2 && err != nil {
			t.Fail()
		}
	}
	defer db.Close()
}

func view(db *badger.DB, key []byte) error {
	return db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		meta := item.UserMeta()
		valueCopy, err := item.ValueCopy(nil)
		fmt.Printf("key[%s] meta[%v] value[%s]\n", key, meta, valueCopy)
		return err
	})
}

func TestSeq(t *testing.T) {
	Init("./data")
	for i := 0; i < 20; i++ {
		next, err := Seq("testseq1", 10)
		fmt.Printf("next: %v, err: %v\n", next, err)
	}
	defer db.Close()
}

func TestMerge(t *testing.T) {
	Init("./data")

	add := func(originalValue, newValue []byte) []byte {
		return append(originalValue, newValue...)
	}

	key := "merget2"
	for i := 'A'; i < 'A'+10; i++ {
		res := Merge(key, string(i), add)
		fmt.Printf("Merge result key:[%s], value:[%s] \n", key, res)
	}
	defer db.Close()
}

func TestAllKey(t *testing.T) {
	Init("./data")
	//All(db)
	//Seek(db, "k-888") // 指定前缀查询
	keys := AllKey()
	for _, k := range keys {
		fmt.Printf("key: %s \n", k)
	}
	defer db.Close()
}
