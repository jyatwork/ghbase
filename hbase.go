//adapter
package ghbase

import (
	"github.com/tsuna/gohbase"
	"github.com/tsuna/gohbase/hrpc"
	"golang.org/x/net/context"
)

type Client struct {
	client gohbase.Client
}

func (c *Client) Get(ctx context.Context, g *hrpc.Get) (*hrpc.Result, error) {
	return c.client.Get(g)
}

func (c *Client) Put(ctx context.Context, p *hrpc.Mutate) (*hrpc.Result, error) {
	return c.client.Put(p)
}

func (c *Client) Scan(ctx context.Context, s *hrpc.Scan) hrpc.Scanner {
	return c.client.Scan(s)
}

func (c *Client) Close() {
	c.client.Close()
}

func NewClient(zkquorum string, options ...gohbase.Option) *Client {
	return &Client{
		client: gohbase.NewClient(zkquorum, options...),
	}
}
