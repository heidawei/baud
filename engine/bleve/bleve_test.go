package bleve

import (
	"testing"
	"github.com/tiglabs/baudengine/engine"
	"os"
	"golang.org/x/net/context"
	"fmt"
	"time"
	"github.com/tiglabs/baudengine/util/json"
)

var bleve_path = "/tmp/baud"


func clear() {
	os.RemoveAll(bleve_path)
}

func blever(t *testing.T, schema string) engine.Engine {
	cfg := engine.EngineConfig{
		Path: bleve_path,
		ReadOnly: false,
		Schema: schema,
	}
	index, err := New(cfg)
	if err != nil {
		t.Fatal(err)
		time.Sleep(time.Second)
	}
	return index
}

func TestWriteDoc(t *testing.T) {
	clear()
	schema := `{
  "mappings": {
    "baud": {
      "properties": {
        "name": {
          "type": "string"
        },
        "age": {
          "type": "integer"
        },
        "baud": {
      "properties": { 
        "title":    { "type": "string"  }, 
        "name":     { "type": "string"  }, 
        "age":      { "type": "integer" }  
      }
    }
      }
    }
  }
}`
	index := blever(t, schema)
	defer func() {
		index.Close()
		clear()
	}()
	type Baud struct {
		Home   string     `json:"home"`
		Title   string     `json:"title"`
		Age    int         `json:"age"`
	}
	err := index.AddDocument(context.Background(), engine.DOC_ID("doc1"), struct {
		Name    string    `json:"name"`
		Age     int        `json:"age"`
		Baud Baud   `json:"address"`
	}{Name: "hewei", Age: 30, Baud: Baud{Home: "beijing", Title: "jingdong", Age: 21}})
	if err != nil {
		t.Fatal(err)
	}
	doc, found := index.GetDocument(context.Background(), engine.DOC_ID("doc1"))
	if !found {
		t.Fatal("get document failed")
	}
	fmt.Println(doc)
}

func TestTermSearchDoc(t *testing.T) {
	clear()
	schema := `{
  "mappings": {
    "baud": {
      "properties": {
        "name": {
          "type": "string"
        },
        "age": {
          "type": "integer"
        },
        "baud": {
      "properties": { 
        "title":    { "type": "string"  }, 
        "name":     { "type": "string"  }, 
        "age":      { "type": "integer" }  
      }
    }
      }
    }
  }
}`
	index := blever(t, schema)
	defer func() {
		index.Close()
		clear()
	}()
	type Baud struct {
		Home   string     `json:"home"`
		Title   string     `json:"title"`
		Age    int         `json:"age"`
	}
	type Home struct {
		Name    string    `json:"name"`
		Age     int        `json:"age"`
		Baud Baud   `json:"address"`
	}
	homes := []Home{
		Home{Name: "hello", Age: 30, Baud: Baud{Home: "beijing", Title: "t8", Age: 21}},
		Home{Name: "yangyang", Age: 23, Baud: Baud{Home: "shanghai", Title: "t5", Age: 23}},
		Home{Name: "dingjun", Age: 33, Baud: Baud{Home: "tianjin", Title: "t5", Age: 30}},
		Home{Name: "yangxiao", Age: 43, Baud: Baud{Home: "dalian", Title: "t6", Age: 43}},
		Home{Name: "meijun", Age: 21, Baud: Baud{Home: "beijing", Title: "t7", Age: 33}},
	}
	for i, home := range homes {
		err := index.AddDocument(context.Background(), engine.DOC_ID(fmt.Sprintf("doc_%d",i+1)), home)
		if err != nil {
			t.Fatal(err)
		}
	}
    res, err := index.Search(context.Background(), &engine.SearchRequest{Index: "index", Type: "baud",
    Size: 10, From: 0,
    Timeout: time.Second, Query: []byte(`{
    "range" : {
        "age" : {
            "gte" : 20,
            "lte" : 30,
            "boost" : 2.0
        }
    }
}`)})
    if err != nil {
    	t.Fatal(err)
    }
    data, _ :=json.Marshal(res)
    fmt.Println(string(data))
}
