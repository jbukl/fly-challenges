package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"

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
		needed_locks := map[string]bool{}

		// find the locks we need
		for _, v := range operations {
			op := v.([]interface{})
			op_type := op[0].(string)
			key := fmt.Sprint(op[1])
			if op_type == "w" {
				needed_locks[key] = true
			}
		}

		// lock loop
		for {
			locked := map[string]bool{}
			for k := range needed_locks {
				err := lin_kv.CompareAndSwap(ctx, fmt.Sprintf("%s-lock", k), 0, 1, true)
				if err != nil {
					break
				}
				locked[k] = true
			}

			if len(locked) == len(needed_locks) {
				break
			}

			for k := range locked {
				_ = lin_kv.CompareAndSwap(ctx, fmt.Sprintf("%s-lock", k), 1, 0, true)
			}
			time.Sleep(time.Duration((rand.Intn(50) + 50)) * time.Millisecond)
		}

		// run ops
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

		// free locks
		for k := range needed_locks {
			_ = lin_kv.CompareAndSwap(ctx, fmt.Sprintf("%s-lock", k), 1, 0, true)
		}

		body["type"] = "txn_ok"

		return n.Reply(msg, body)
	})
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
