package dnsregistry

import "net"

type dnsResolver struct {
}

// NewDNSResolver returns a new DNSResolver that is backed by the
// standard library implementation of net.LookupIP.
func NewDNSResolver() DNSResolver {
	return &dnsResolver{}
}

func (d *dnsResolver) LookupIP(host string) ([]net.IP, error) {
	return net.LookupIP(host)
}
