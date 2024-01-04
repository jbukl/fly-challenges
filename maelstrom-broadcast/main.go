package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	message_map := make(map[float64]bool)
	messages := make([]float64, 0)
	topology := make([]string, 0)

	queue := make([]float64, 0, 10)

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		message := body["message"].(float64)
		_, ok := message_map[message]
		if !ok {
			message_map[message] = true
			messages = append(messages, message)
			queue = append(queue, message)
		}
		resp := map[string]interface{}{
			"type": "broadcast_ok",
		}
		return n.Reply(msg, resp)
	})

	n.Handle("broadcast_node", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		recv_messages := make([]float64, 0)

		interfaceArr := body["messages"]
		switch v := interfaceArr.(type) {
		case []interface{}:
			for x := 0; x < len(v); x++ {
				recv_messages = append(recv_messages, v[x].(float64))
			}
		default:
			fmt.Printf("invalid type: %T\n", v)
		}
		for _, message := range recv_messages {
			_, ok := message_map[message]
			if !ok {
				message_map[message] = true
				messages = append(messages, message)
				queue = append(queue, message)
			}
		}
		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		messages := make([]float64, 0, len(message_map))
		for k := range message_map {
			messages = append(messages, k)
		}

		resp := map[string]interface{}{
			"type":     "read_ok",
			"messages": messages,
		}

		return n.Reply(msg, resp)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		interfaceArr := body["topology"].(map[string]interface{})[n.ID()]

		topology = make([]string, 0)
		switch v := interfaceArr.(type) {
		case []interface{}:
			for x := 0; x < len(v); x++ {
				topology = append(topology, v[x].(string))
			}
		default:
			fmt.Printf("invalid type: %T\n", v)
		}

		resp := map[string]interface{}{
			"type": "topology_ok",
		}

		return n.Reply(msg, resp)
	})

	ticker := time.NewTicker(500 * time.Millisecond)
	go func() {
		for range ticker.C {
			if len(queue) != 0 {
				body := map[string]interface{}{"type": "broadcast_node", "messages": queue}
				for _, neighbor := range topology {
					n.Send(neighbor, body)
				}
				queue = make([]float64, 0)
			}
		}
	}()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
