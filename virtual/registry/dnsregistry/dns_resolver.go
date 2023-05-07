package dnsregistry

import (
	"fmt"
	"net"

	"github.com/richardartoul/nola/virtual/registry"
)

type dnsResolver struct {
	port int
}

// NewDNSResolver returns a new DNSResolver that is backed by the
// standard library implementation of net.LookupIP.
func NewDNSResolver(port int) DNSResolver {
	return &dnsResolver{
		port: port,
	}
}

func (d *dnsResolver) LookupIP(host string) ([]registry.Address, error) {
	ips, err := net.LookupIP(host)
	if err != nil {
		return nil, fmt.Errorf("error in net.LookupIP: %w", err)
	}

	addrs := make([]registry.Address, 0, len(ips))
	for _, ip := range ips {
		addrs = append(addrs, registry.Address{IP: ip, Port: d.port})
	}

	return addrs, nil
}
