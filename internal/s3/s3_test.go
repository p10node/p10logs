package s3

import (
	"context"
	"net/http/httptest"
	"testing"
)

func TestClientAgainstFake(t *testing.T) {
	srv := httptest.NewServer(NewFake())
	defer srv.Close()
	c, err := New(Config{Endpoint: srv.URL, Bucket: "b", AccessKey: "a", SecretKey: "s", PathStyle: true, Prefix: "p10"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := c.EnsureBucket(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.Put(ctx, "days/x/1.chunk", []byte("0123456789")); err != nil {
		t.Fatal(err)
	}
	b, err := c.Get(ctx, "days/x/1.chunk", 0, -1)
	if err != nil || string(b) != "0123456789" {
		t.Fatalf("get %q %v", b, err)
	}
	b, err = c.Get(ctx, "days/x/1.chunk", 7, -1)
	if err != nil || string(b) != "789" {
		t.Fatalf("range %q %v", b, err)
	}
	objs, err := c.List(ctx, "days/")
	if err != nil || len(objs) != 1 || objs[0].Key != "days/x/1.chunk" || objs[0].Size != 10 {
		t.Fatalf("list %+v %v", objs, err)
	}
	if err := c.Delete(ctx, "days/x/1.chunk"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Get(ctx, "days/x/1.chunk", 0, -1); err != ErrNotFound {
		t.Fatalf("want not found, got %v", err)
	}
}
