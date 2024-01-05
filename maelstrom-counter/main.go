package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)
	ctx := context.Background()

	n.Handle("add", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		delta := int(body["delta"].(float64))
		prevVal := 0
		for {
			err := kv.CompareAndSwap(ctx, "counter", prevVal, prevVal+delta, true)
			if err == nil {
				break
			}
			prevVal, _ = kv.ReadInt(ctx, "counter")
			time.Sleep(10 * time.Millisecond)
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

		_ = kv.CompareAndSwap(ctx, "counter", 0, 0, true)
		val, _ := kv.ReadInt(ctx, "counter")

		body["type"] = "read_ok"
		body["value"] = val

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
