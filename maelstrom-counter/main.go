package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func sleepChan(neighbor string, c chan string) {
	time.Sleep(50 * time.Millisecond)
	c <- neighbor
}

func initialize_list(n *maelstrom.Node, c chan string) {
	for _, neigh := range n.NodeIDs() {
		c <- neigh
	}
}

func main() {
	n := maelstrom.NewNode()
	node_counters := map[string]int{}
	list_initialized := false
	c := make(chan string)
	var mu sync.Mutex

	n.Handle("add", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		delta := int(body["delta"].(float64))

		mu.Lock()
		node_counters[n.ID()] += delta
		mu.Unlock()

		body["type"] = "add_ok"
		delete(body, "delta")

		return n.Reply(msg, body)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		if !list_initialized {
			initialize_list(n, c)
			list_initialized = true
		}

		read_total := 0
		mu.Lock()
		for _, ctr := range node_counters {
			read_total += ctr
		}
		mu.Unlock()

		body["type"] = "read_ok"
		body["value"] = read_total

		return n.Reply(msg, body)
	})

	n.Handle("sniff_node", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		counters_copy := map[string]int{}
		mu.Lock()
		for k, v := range node_counters {
			counters_copy[k] = v
		}
		mu.Unlock()

		body["type"] = "sniff_ok"
		body["counters"] = counters_copy

		return n.Reply(msg, body)
	})

	go func() {
		for neighbor := range c {
			go sleepChan(neighbor, c)

			body := map[string]interface{}{"type": "sniff_node"}

			neigh := neighbor
			n.RPC(neigh, body, func(msg maelstrom.Message) error {
				var body map[string]any
				if err := json.Unmarshal(msg.Body, &body); err != nil {
					return err
				}
				v := body["counters"]
				neigh_counters, ok := v.(map[string]interface{})
				if !ok {
					return nil
				}

				mu.Lock()
				for k, v := range neigh_counters {
					if _, ok := node_counters[k]; !ok {
						node_counters[k] = 0
					}
					node_counters[k] = max(node_counters[k], int(v.(float64)))
				}
				mu.Unlock()
				return nil
			})
		}
	}()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
