package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func sleepChan(neighbor string, c chan string) {
	time.Sleep(50 * time.Millisecond)
	c <- neighbor
}

func main() {
	n := maelstrom.NewNode()
	message_map := make(map[float64]int) // message: idx in local messages list
	messages := make([]float64, 0)
	topology := make([]string, 0)
	acked_map := make(map[string]int) // node_id: latest acked index into messages
	var mu sync.Mutex

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		message := body["message"].(float64)
		mu.Lock()
		_, ok := message_map[message]
		if !ok {
			message_map[message] = len(messages)
			messages = append(messages, message)
		}
		mu.Unlock()
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
			mu.Lock()
			_, ok := message_map[message]
			if !ok {
				message_map[message] = len(messages)
				messages = append(messages, message)
			}
			mu.Unlock()
		}
		resp := map[string]interface{}{
			"type": "broadcast_node_ok",
			"ack":  recv_messages[len(recv_messages)-1],
			"node": n.ID(),
		}
		return n.Reply(msg, resp)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		mu.Lock()
		resp := map[string]interface{}{
			"type":     "read_ok",
			"messages": append([]float64{}, messages...),
		}
		mu.Unlock()

		return n.Reply(msg, resp)
	})

	c := make(chan string)
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
				neighbor := v[x].(string)
				topology = append(topology, neighbor)
				acked_map[neighbor] = -1
				fmt.Fprintf(os.Stderr, "node: %s, neighbor: %s\n", n.ID(), neighbor)
				c <- neighbor
			}
		default:
			fmt.Printf("invalid type: %T\n", v)
		}

		resp := map[string]interface{}{
			"type": "topology_ok",
		}

		return n.Reply(msg, resp)
	})

	go func() {
		for neighbor := range c {
			go sleepChan(neighbor, c)
			mu.Lock()
			neighbor_ack := acked_map[neighbor]
			if neighbor_ack == len(messages)-1 {
				mu.Unlock()
				continue
			}
			body := map[string]interface{}{"type": "broadcast_node", "messages": append([]float64{}, messages[neighbor_ack+1:]...)} // messages[neighbor_ack+1 : neighbor_ack+1+send_count]}
			mu.Unlock()

			neigh := neighbor
			n.RPC(neigh, body, func(msg maelstrom.Message) error {
				var body map[string]any
				if err := json.Unmarshal(msg.Body, &body); err != nil {
					return err
				}
				ack_msg := body["ack"].(float64)
				mu.Lock()
				acked_map[neigh] = max(message_map[ack_msg], acked_map[neigh])
				mu.Unlock()
				return nil
			})
		}
	}()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
