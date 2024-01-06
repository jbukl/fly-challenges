package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	logs := map[string][]int{}
	committed_offsets := map[string]int{}
	var mu sync.Mutex

	n.Handle("send", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		key := body["key"].(string)
		recv_msg := int(body["msg"].(float64))

		mu.Lock()
		resp_body := map[string]interface{}{
			"type":   "send_ok",
			"offset": len(logs[key]),
		}
		logs[key] = append(logs[key], recv_msg)
		mu.Unlock()

		return n.Reply(msg, resp_body)
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		req_offsets := body["offsets"].(map[string]interface{})

		// fmt.Fprintln(os.Stderr, logs)
		send_msgs := map[string][][]int{}
		mu.Lock()
		for k, v := range req_offsets {
			req_offset := int(v.(float64))
			send_offset := max(0, min(5, len(logs[k])-req_offset))

			send_msgs[k] = [][]int{}
			for i := req_offset; i < req_offset+send_offset; i++ {
				send_msgs[k] = append(send_msgs[k], []int{i, logs[k][i]})
			}
		}
		mu.Unlock()

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

		committed_offsets = map[string]int{}
		mu.Lock()
		for k, v := range recv_offsets {
			committed_offsets[k] = max(committed_offsets[k], int(v.(float64)))
		}
		mu.Unlock()

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

		committed_copy := map[string]int{}
		mu.Lock()
		for k, v := range committed_offsets {
			committed_copy[k] = v
		}
		mu.Unlock()

		resp_body := map[string]interface{}{
			"type":    "list_committed_offsets_ok",
			"offsets": committed_copy,
		}

		return n.Reply(msg, resp_body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
