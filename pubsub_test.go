package predis

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
)

func TestConn_Subscribe(t *testing.T) {
	InitPool("127.0.0.1:6379", "", WithDialOptions(redis.DialReadTimeout(time.Second)))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	conn, err := NewConn(ctx, "127.0.0.1:6379", "")
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		conn := GetConn()
		defer conn.Release()

		for i := 0; i <= 10; i++ {
			_, err := conn.Publish("tp", []byte(strconv.Itoa(i)))
			if err != nil {
				t.Fatal(err)
			}
		}
	}()

	ch, errCh, err := conn.Subscribe(ctx, 0, "tp")
	if err != nil {
		t.Fatal(err)
	}
	for m := range ch {
		t.Log(string(m.Data))
	}
	t.Log("end")
	t.Log(<-errCh)
}
