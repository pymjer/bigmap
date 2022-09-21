package bigmap

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/ristretto/z"
)

var db *badger.DB

func Init(dbPath string) error {
	var err error

	db, err = badger.Open(badger.DefaultOptions(dbPath))
	if err != nil {
		return err
	}
	return nil
}

// Set用于在db中添加一个键值对，如果添加失败会返回一个错误
func Set(k string, v string) error {
	txn := db.NewTransaction(true)
	defer txn.Discard()
	err := txn.Set([]byte(k), []byte(v))
	txn.Commit()
	return err
}

// SetWithTTL用于在db中添加一个有失效的键值对，时间单位为秒
func SetWithTTL(key string, val string, second int) {
	db.Update(func(txn *badger.Txn) error {
		e := badger.NewEntry([]byte(key), []byte(val)).
			WithTTL(time.Second * time.Duration(second)).
			WithMeta(byte(3))
		err := txn.SetEntry(e)
		return err
	})
}

// Query
func Query(k string) (string, error) {
	txn := db.NewTransaction(false)
	item, err := txn.Get([]byte(k))
	if err != nil {
		return "", err
	}
	valCopy, err := item.ValueCopy(nil)
	if err != nil {
		log.Fatal(err)
	}
	return string(valCopy), nil
}

// Delete
func Delete(k string) error {
	txn := db.NewTransaction(true)
	defer txn.Discard()
	err := txn.Delete([]byte(k))
	txn.Commit()
	return err
}

// Sequence
func Seq(key string, bandwidth int) (uint64, error) {
	seq, _ := db.GetSequence([]byte(key), uint64(bandwidth))
	defer seq.Release()
	return seq.Next()
}

// Merge
func Merge(key string, mvalue string, f badger.MergeFunc) string {
	m := db.GetMergeOperator([]byte(key), f, 200*time.Millisecond)
	defer m.Stop()
	m.Add([]byte(mvalue))
	res, _ := m.Get()
	return string(res)
}

// 返回库中的所有key
func AllKey() []string {
	res := []string{}
	db.View(func(txn *badger.Txn) error {
		options := badger.DefaultIteratorOptions
		options.PrefetchValues = false // 是否取值，如果不取可以快很多，然后在循环的时候取
		it := txn.NewIterator(options)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			res = append(res, string(item.Key()))
		}
		return nil
	})
	return res
}

type KVPair struct {
	key   string
	value string
}

func (p KVPair) String() string {
	return fmt.Sprintf("%s:%s", p.key, p.value)
}

func Seek(prefix string) []KVPair {
	res := []KVPair{}
	db.View(func(txn *badger.Txn) error {
		options := badger.DefaultIteratorOptions
		options.PrefetchSize = 10
		it := txn.NewIterator(options)
		defer it.Close()
		prefixByte := []byte(prefix)
		for it.Seek(prefixByte); it.ValidForPrefix(prefixByte); it.Next() {
			item := it.Item()
			valCopy, _ := item.ValueCopy(nil)
			res = append(res, KVPair{string(item.Key()), string(valCopy)})
		}
		return nil
	})
	return res
}

// 打印所有的键值对
func All() []KVPair {
	res := []KVPair{}
	db.View(func(txn *badger.Txn) error {
		options := badger.DefaultIteratorOptions
		options.PrefetchSize = 10

		it := txn.NewIterator(options)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			valCopy, _ := item.ValueCopy(nil)
			res = append(res, KVPair{string(item.Key()), string(valCopy)})
		}

		return nil
	})
	return res
}

func Stream(prefix string, send func(buf *z.Buffer) error) error {
	stream := db.NewStream()

	// -- Optional setting
	stream.NumGo = 16
	stream.Prefix = []byte(prefix)
	stream.LogPrefix = "Badger.Streaming"
	stream.KeyToList = nil // convert badger data into custom key-values

	stream.Send = send

	if err := stream.Orchestrate(context.Background()); err != nil {
		return err
	}
	return nil
}

func uint64ToBytes(i uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], i)
	return buf[:]
}

func bytesToUint64(b []byte) uint64 {
	//fmt.Println(b)
	return binary.BigEndian.Uint64(b)
}
