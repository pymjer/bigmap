// 这个示例演示如何使用Bigmap
package bigmap_test

import (
	"fmt"
	"log"

	"github.com/pymjer/bigmap"
)

type KVP struct {
	key, val string
}

// 这个示例打开一个默认的kv数据库，添加一些值，然后做查询操作
func Example_kVP() {
	path := "./data"
	bigmap.Init(path)

	var tests = []KVP{
		{"t1", "v1"},
		{"t2", "v2"},
		{"t3", "v3"},
	}

	for _, tt := range tests {
		bigmap.Set(tt.key, tt.val)
		ans, err := bigmap.Query(tt.key)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s:%s ", tt.key, ans)
	}
	// Output:
	// t1:v1 t2:v2 t3:v3
}
