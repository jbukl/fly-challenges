package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	lin_kv := maelstrom.NewLinKV(n)
	ctx := context.Background()

	n.Handle("txn", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		operations := body["txn"].([]interface{})
		for _, v := range operations {
			op := v.([]interface{})
			op_type := op[0].(string)
			key := fmt.Sprint(op[1])
			if op_type == "w" {
				lin_kv.Write(ctx, key, op[2])
			} else {
				val, err := lin_kv.Read(ctx, key)
				if err == nil {
					op[2] = val
				}
			}
		}

		body["type"] = "txn_ok"

		return n.Reply(msg, body)
	})
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
