package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func initialize_list(ctx context.Context, kv *maelstrom.KV, n *maelstrom.Node) {
	_ = kv.CompareAndSwap(ctx, "node_list", []string{}, []string{}, true)

	for {
		x, _ := kv.Read(ctx, "node_list")
		interface_arr := x.([]interface{})

		node_list := []string{}
		for _, y := range interface_arr {
			node_list = append(node_list, y.(string))
		}
		node_list = append(node_list, n.ID())

		if err := kv.CompareAndSwap(ctx, "node_list", x, node_list, false); err == nil {
			break
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func read_nodes(ctx context.Context, kv *maelstrom.KV) []string {
	x, _ := kv.Read(ctx, "node_list")
	interface_arr := x.([]interface{})

	node_list := []string{}
	for _, y := range interface_arr {
		node_list = append(node_list, y.(string))
	}
	return node_list
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)
	ctx := context.Background()
	node_counter := 0
	list_initialized := false

	n.Handle("add", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		delta := int(body["delta"].(float64))
		node_counter += delta
		if err := kv.Write(ctx, fmt.Sprintf("%s-counter", n.ID()), node_counter); err != nil {
			return err
		}

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
			initialize_list(ctx, kv, n)
			list_initialized = true
		}

		node_list := read_nodes(ctx, kv)

		read_total := 0
		for _, neigh := range node_list {
			if neigh == n.ID() {
				read_total += node_counter
				continue
			}
			val, err := kv.ReadInt(ctx, fmt.Sprintf("%s-counter", neigh))
			if err == nil {
				read_total += val
			}
		}
		body["type"] = "read_ok"
		body["value"] = read_total

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
