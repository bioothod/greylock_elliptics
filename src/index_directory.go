package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Paging struct {
	Num		int64		`json:"num"`
	Start		string		`jsin:"string"`
}

type Timestamp struct {
	Tsec		int64		`json:"tsec"`
	Tnsec		int64		`json:"tnsec"`
}

type Index struct {
	Text		string		`json:"text"`
}

type Document struct {
	Id		string		`json:"id"`
	Bucket		string		`json:"bucket"`
	Key		string		`json:"key"`
	Timestamp	Timestamp	`json:"timestamp"`
	Index		Index		`json:"index"`
}

type Query struct {
	Text		string		`json:"text"`
}

type IndexReq struct {
	Mailbox		string		`json:"mailbox"`
	Docs		[]interface{}	`json:"docs"`
}

type SearchReq struct {
	Mailbox		string		`json:"mailbox"`
	Query		interface{}	`json:"query"`
}

type ResponseID struct {
	Id		string		`json:"id"`
	Bucket		string		`json:"bucket"`
	Key		string		`json:"key"`
	Timestamp	Timestamp	`json:"timestamp"`
	Relevance	float64		`json:"relevance"`
}

type SearchResponse struct {
	Completed	bool		`json:"completed"`
	Paging		Paging		`json:"paging"`
	Responses	[]ResponseID	`json:"ids"`
}

var count int = 0
var prev_search_count int = 0

func check_search(mailbox, query string) {
	sreq := SearchReq {
		Mailbox:	mailbox,
		Query:		Query {
					Text:	query,
				},
	}

	s, err := json.Marshal(sreq)
	if err != nil {
		panic(err)
	}

	url := "http://localhost:8080/search"
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(s))
	if err != nil {
		panic(err)
	}

	client := &http.Client{}

	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		panic(fmt.Sprintf("Bad search response code: ", resp.StatusCode))
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}

	var sr SearchResponse
	err = json.Unmarshal(body, &sr)
	if err != nil {
		log.Fatalf("unmarshal error: body: '%s', size: %d, resp: %v, err: %v\n", string(body), len(body), resp, err)
	}

	if len(sr.Responses) >= prev_search_count {
		prev_search_count = len(sr.Responses)
		return
	}

	log.Printf("invalid search response count: %d, must be more than previous: %d\n", len(sr.Responses), prev_search_count)
	log.Printf("response: '%s'", string(body))
	os.Exit(-1)
}

func send_index_request(path string) error {
	t := Timestamp{time.Now().Unix(), 1234}
	data, err := ioutil.ReadFile(path)
	if err != nil {
		log.Printf("Could not open file %s: %v\n", path, err)
		return err
	}

	res := strings.Replace(string(data), "\r", "", -1)
	res = strings.Replace(res, "\n", " ", -1)

	if len(data) > 2 * 1024 * 1024 {
		log.Printf("File %s is too large: size: %d\n", path, len(data))
		return nil
	}

	doc := Document {
		Id:		path,
		Bucket:		"unused",
		Key:		"unused",
		Timestamp:	t,
		Index:		Index {
					Text:	res,
				},
	}

	ireq := IndexReq{}
	ireq.Docs = append(ireq.Docs, doc)
	ireq.Mailbox = "testtest"

	s, err := json.Marshal(ireq)
	if err != nil {
		panic(err)
	}
	//ioutil.WriteFile("request.json", s, 0777)

	start := time.Now()

	url := "http://localhost:8080/index"
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(s))
	if err != nil {
		panic(err)
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	fmt.Printf("%s: %s: filesize: %d ... ", start.String(), path, len(res))

	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	_, _ = ioutil.ReadAll(resp.Body)

	count++

	check_search(ireq.Mailbox, "dnet_usage main")

	fmt.Printf("count: %d, search responses: %d, status: %s %s\n",
		count, prev_search_count, resp.Status, time.Since(start))

	if resp.StatusCode != 200 {
		fmt.Println("Bad response code: ", resp.StatusCode)
		os.Exit(-1)
	}

	return nil
}

func index(path string) {
	if strings.HasSuffix(path, ".go") {
		send_index_request(path)
	}
	if strings.HasSuffix(path, ".cpp") {
		send_index_request(path)
	}
	if strings.HasSuffix(path, ".hpp") {
		send_index_request(path)
	}
	if strings.HasSuffix(path, ".c") {
		send_index_request(path)
	}
	if strings.HasSuffix(path, ".h") {
		send_index_request(path)
	}
	if strings.HasSuffix(path, ".txt") {
		send_index_request(path)
	}
}

func walk(root string) {
	filepath.Walk(root, walcfunc)
}

func walcfunc (path string, info os.FileInfo, err error) error {
	if info.IsDir() {
		return nil
	}

	index(path)
	return nil
}

func main() {
	dir := flag.String("dir", "./", "Directory to traverse and parse files")
	list := flag.String("list", "", "File containing list of files to parse")
	flag.Parse()

	count = 0

	if *list != "" {
		lcontent, err := ioutil.ReadFile(*list)
		if err != nil {
			log.Fatalf("Could not open list file %s: %v\n", *list, err)
		}

		for _, file := range strings.Split(string(lcontent), "\n") {
			index(file)
		}

		return
	}

	walk(*dir)
}
