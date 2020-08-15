package main

import (
	"context"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	proto "github.com/micro/examples/stream/server/proto"
	"github.com/micro/go-micro/v2/client"
	grpcclient "github.com/micro/go-micro/v2/client/grpc"
	"github.com/micro/go-micro/v2/web"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func Stream(cli proto.StreamerService, ws *websocket.Conn) error {
	// Read initial request from websocket
	var req proto.Request
	err := ws.ReadJSON(&req)
	if err != nil {
		return err
	}

	// Even if we aren't expecting further requests from the websocket, we still need to read from it to ensure we
	// get close signals
	go func() {
		for {
			if _, _, err := ws.NextReader(); err != nil {
				break
			}
		}
	}()

	log.Printf("Received Request: %v", req)

	// Send request to stream server
	stream, err := cli.ServerStream(context.Background(), &req)
	if err != nil {
		log.Printf("cli.ServerStream err: %v", err)
		return err
	}
	defer stream.Close()

	// Read from the stream server and pass responses on to websocket
	for {
		// Read from stream, end request once the stream is closed
		rsp, err := stream.Recv()
		if err != nil {
			if err != io.EOF {
				log.Printf("stream.Recv err: %v", err)
				return err
			}
			log.Println("stream.Recv EOF")
			break
		}

		// Write server response to the websocket
		err = ws.WriteJSON(rsp)
		if err != nil {
			// End request if socket is closed
			if isExpectedClose(err) {
				log.Println("Expected Close on socket", err)
				break
			} else {
				log.Println("ws.WriteJSON err: ", err)
				return err
			}
		}
	}

	return nil
}

func isExpectedClose(err error) bool {
	if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway) {
		log.Println("Unexpected websocket close: ", err)
		return false
	}

	return true
}

func main() {
	// New web service
	service := web.NewService(
		web.Name("go.micro.web.stream"),
	)

	if err := service.Init(); err != nil {
		log.Fatal("Init", err)
	}

	// New RPC client
	rpcClient := grpcclient.NewClient(client.RequestTimeout(time.Second * 120))
	cli := proto.NewStreamerService("go.micro.srv.stream", rpcClient)

	// Serve static html/js
	service.Handle("/", http.FileServer(http.Dir("html")))

	// Handle websocket connection
	service.HandleFunc("/stream", func(w http.ResponseWriter, r *http.Request) {
		// Upgrade request to websocket
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Fatal("Upgrade: ", err)
			return
		}
		defer conn.Close()

		// Handle websocket request
		if err := Stream(cli, conn); err != nil {
			log.Fatal("Echo: ", err)
			return
		}
		log.Println("Stream complete")
	})

	if err := service.Run(); err != nil {
		log.Fatal("Run: ", err)
	}
}
