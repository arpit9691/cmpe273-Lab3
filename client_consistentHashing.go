package main

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"net/http"
	"os"
	"sort"
	"strings"
)

type HashRing []uint32

type KeyVal struct {
	Key   int    `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

func (hr HashRing) Len() int {
	return len(hr)
}

func (hr HashRing) Less(i, j int) bool {
	return hr[i] < hr[j]
}

func (hr HashRing) Swap(i, j int) {
	hr[i], hr[j] = hr[j], hr[i]
}

type Node struct {
	Id int
	IP string
}

func NewNode(id int, ip string) *Node {
	return &Node{
		Id: id,
		IP: ip,
	}
}

type ConsistentHashing struct {
	Nodes     map[uint32]Node
	IsPresent map[int]bool
	Ring      HashRing
}

func NewConsistentHashing() *ConsistentHashing {
	return &ConsistentHashing{
		Nodes:     make(map[uint32]Node),
		IsPresent: make(map[int]bool),
		Ring:      HashRing{},
	}
}

func (hr *ConsistentHashing) AddNode(node *Node) bool {

	if _, ok := hr.IsPresent[node.Id]; ok {
		return false
	}
	str := hr.ReturnNodeIP(node)
	hr.Nodes[hr.GetHashVal(str)] = *(node)
	hr.IsPresent[node.Id] = true
	hr.SortHashRing()
	return true
}

func (hr *ConsistentHashing) SortHashRing() {
	hr.Ring = HashRing{}
	for k := range hr.Nodes {
		hr.Ring = append(hr.Ring, k)
	}
	sort.Sort(hr.Ring)
}

func (hr *ConsistentHashing) ReturnNodeIP(node *Node) string {
	return node.IP
}

func (hr *ConsistentHashing) GetHashVal(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

func (hr *ConsistentHashing) Get(key string) Node {
	hashVal := hr.GetHashVal(key)
	i := hr.SearchForNode(hashVal)
	return hr.Nodes[hr.Ring[i]]
}

func (hr *ConsistentHashing) SearchForNode(hash uint32) int {
	i := sort.Search(len(hr.Ring), func(i int) bool { return hr.Ring[i] >= hash })
	if i < len(hr.Ring) {
		if i == len(hr.Ring)-1 {
			return 0
		} else {
			return i
		}
	} else {
		return len(hr.Ring) - 1
	}
}

func PutKey(ring *ConsistentHashing, str string, input string) {
	ipAddress := ring.Get(str)
	address := "http://" + ipAddress.IP + "/keys/" + str + "/" + input
	fmt.Println(address)
	req, err := http.NewRequest("PUT", address, nil)
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Error:", err)
	} else {
		defer resp.Body.Close()
		fmt.Println("PUT Request successfully completed")
	}
}

func GetKey(key string, ring *ConsistentHashing) {
	var out KeyVal
	ipAddress := ring.Get(key)
	address := "http://" + ipAddress.IP + "/keys/" + key
	fmt.Println(address)
	response, err := http.Get(address)
	if err != nil {
		fmt.Println("Error:", err)
	} else {
		defer response.Body.Close()
		contents, err := ioutil.ReadAll(response.Body)
		if err != nil {
			fmt.Println(err)
		}
		json.Unmarshal(contents, &out)
		result, _ := json.Marshal(out)
		fmt.Println(string(result))
	}
}

func GetAll(address string) {

	var out []KeyVal
	response, err := http.Get(address)
	if err != nil {
		fmt.Println("Error:", err)
	} else {
		defer response.Body.Close()
		contents, err := ioutil.ReadAll(response.Body)
		if err != nil {
			fmt.Println(err)
		}
		json.Unmarshal(contents, &out)
		result, _ := json.Marshal(out)
		fmt.Println(string(result))
	}
}
func main() {
	ring := NewConsistentHashing()
	ring.AddNode(NewNode(0, "127.0.0.1:3000"))
	ring.AddNode(NewNode(1, "127.0.0.1:3001"))
	ring.AddNode(NewNode(2, "127.0.0.1:3002"))

	if os.Args[1] == "PUT" {
		key := strings.Split(os.Args[2], "/")
		PutKey(ring, key[0], key[1])
	} else if (os.Args[1] == "GET") && len(os.Args) == 3 {
		GetKey(os.Args[2], ring)
	} else {
		GetAll("http://127.0.0.1:3000/keys")
		GetAll("http://127.0.0.1:3001/keys")
		GetAll("http://127.0.0.1:3002/keys")
	}
}
