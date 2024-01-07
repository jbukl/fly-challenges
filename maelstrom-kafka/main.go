package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	lin_kv := maelstrom.NewLinKV(n)
	seq_kv := maelstrom.NewSeqKV(n)
	ctx := context.Background()

	n.Handle("send", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		key := body["key"].(string)
		recv_msg := int(body["msg"].(float64))

		// linearizable increment offset increment, then insert msg into seq_kv
		_ = lin_kv.CompareAndSwap(ctx, fmt.Sprintf("%s-size", key), 0, 0, true)
		var offset int
		for {
			offset, _ = lin_kv.ReadInt(ctx, fmt.Sprintf("%s-size", key))
			err := lin_kv.CompareAndSwap(ctx, fmt.Sprintf("%s-size", key), offset, offset+1, false)
			if err == nil {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		_ = seq_kv.Write(ctx, fmt.Sprintf("%s-%d", key, offset), recv_msg)

		resp_body := map[string]interface{}{
			"type":   "send_ok",
			"offset": offset,
		}

		return n.Reply(msg, resp_body)
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		req_offsets := body["offsets"].(map[string]interface{})

		send_msgs := map[string][][]int{}
		for k, v := range req_offsets {
			req_offset := int(v.(float64))
			send_size := 5

			send_msgs[k] = [][]int{}
			for i := req_offset; i < req_offset+send_size; i++ {
				val, err := seq_kv.ReadInt(ctx, fmt.Sprintf("%s-%d", k, i))
				if err != nil {
					break
				}

				send_msgs[k] = append(send_msgs[k], []int{i, val})
			}
		}

		resp_body := map[string]interface{}{
			"type": "poll_ok",
			"msgs": send_msgs,
		}

		return n.Reply(msg, resp_body)
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		recv_offsets := body["offsets"].(map[string]interface{})

		_ = lin_kv.CompareAndSwap(ctx, "committed_offsets", map[string]int{}, map[string]int{}, true)
		for {
			committed_interface, _ := lin_kv.Read(ctx, "committed_offsets")
			committed_offsets := committed_interface.(map[string]interface{})
			for k, v := range committed_offsets {
				recv_v, _ := recv_offsets[k].(float64)
				recv_offsets[k] = max(int(recv_v), int(v.(float64)))
			}
			err := lin_kv.CompareAndSwap(ctx, "committed_offsets", committed_interface, recv_offsets, false)
			if err == nil {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}

		resp_body := map[string]interface{}{
			"type": "commit_offsets_ok",
		}

		return n.Reply(msg, resp_body)
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		_ = lin_kv.CompareAndSwap(ctx, "committed_offsets", map[string]int{}, map[string]int{}, true)
		committed_interface, _ := lin_kv.Read(ctx, "committed_offsets")

		resp_body := map[string]interface{}{
			"type":    "list_committed_offsets_ok",
			"offsets": committed_interface,
		}

		return n.Reply(msg, resp_body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
