package main

import (
	//"github.com/julienschmidt/httprouter"
	"encoding/json"
	"fmt"
	"httprouter"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

type KeyVal struct {
	Key   int    `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

var a1, a2, a3 []KeyVal
var i1, i2, i3 int

type Key []KeyVal

func (a Key) Len() int           { return len(a) }
func (a Key) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a Key) Less(i, j int) bool { return a[i].Key < a[j].Key }

func GetAllKeys(rw http.ResponseWriter, request *http.Request, p httprouter.Params) {
	port := strings.Split(request.Host, ":")
	if port[1] == "3000" {
		sort.Sort(Key(a1))
		result, _ := json.Marshal(a1)
		fmt.Fprintln(rw, string(result))
	} else if port[1] == "3001" {
		sort.Sort(Key(a2))
		result, _ := json.Marshal(a2)
		fmt.Fprintln(rw, string(result))
	} else {
		sort.Sort(Key(a3))
		result, _ := json.Marshal(a3)
		fmt.Fprintln(rw, string(result))
	}
}

func PutKeys(rw http.ResponseWriter, request *http.Request, p httprouter.Params) {
	port := strings.Split(request.Host, ":")
	key, _ := strconv.Atoi(p.ByName("key_id"))
	if port[1] == "3000" {
		a1 = append(a1, KeyVal{key, p.ByName("value")})
		i1++
	} else if port[1] == "3001" {
		a2 = append(a2, KeyVal{key, p.ByName("value")})
		i2++
	} else {
		a3 = append(a3, KeyVal{key, p.ByName("value")})
		i3++
	}
}

func GetKey(rw http.ResponseWriter, request *http.Request, p httprouter.Params) {
	out := a1
	index := i1
	port := strings.Split(request.Host, ":")
	if port[1] == "3001" {
		out = a2
		index = i2
	} else if port[1] == "3002" {
		out = a3
		index = i3
	}
	key, _ := strconv.Atoi(p.ByName("key_id"))
	for i := 0; i < index; i++ {
		if out[i].Key == key {
			result, _ := json.Marshal(out[i])
			fmt.Fprintln(rw, string(result))
		}
	}
}

func main() {
	i1 = 0
	i2 = 0
	i3 = 0
	mux := httprouter.New()
	mux.GET("/keys", GetAllKeys)
	mux.GET("/keys/:key_id", GetKey)
	mux.PUT("/keys/:key_id/:value", PutKeys)
	go http.ListenAndServe(":3000", mux)
	go http.ListenAndServe(":3001", mux)
	go http.ListenAndServe(":3002", mux)
	select {}
}
