package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/lwangrabbit/prom-query/api"
)

func main() {
	// Init
	urls := []string{
		"localhost:9090",
		"localhost:9091",
	}
	var configs []*api.ReadConfig
	for _, url := range urls {
		configs = append(configs, &api.ReadConfig{
			URL:     fmt.Sprintf("http://%v", url),
			Timeout: 30 * time.Second,
		})
	}
	api.Init(configs)

	query := `up`
	res, err := api.Query(query)
	if err != nil {
		panic(err)
	}
	bs, _ := json.Marshal(res)
	log.Println("instant query result: ", string(bs))

	endTs := time.Now().Unix()
	startTs := endTs - 300
	res, err = api.QueryRange(query, startTs, endTs, 60)
	if err != nil {
		panic(err)
	}
	bs, _ = json.Marshal(res)
	log.Println("range query result: ", string(bs))
}
