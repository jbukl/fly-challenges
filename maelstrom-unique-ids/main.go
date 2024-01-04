package main

import (
	"fmt"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	counter := 0
	n.Handle("generate", func(msg maelstrom.Message) error {
		resp := map[string]interface{}{
			"type": "generate_ok",
			"id":   fmt.Sprintf("%s-%d", n.ID(), counter),
		}
		counter++

		// Echo the original message back with the updated message type.
		return n.Reply(msg, resp)
	})
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
