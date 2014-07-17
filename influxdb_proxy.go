package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/influxdb/influxdb-go"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"
)

const UDPMaxMessageSize = 2048 // max bytes we can send to InfluxDB.
                               // see: https://github.com/influxdb/influxdb/blob/master/api/udp/api.go

func main() {
	var (
		laddr    string
		raddr    string
		flushInt time.Duration
		test     bool
	)
	flag.StringVar(&laddr, "laddr", ":2004", "local address to bind to")
	flag.StringVar(&raddr, "raddr", ":5551", "remote address to proxy to")
	flag.DurationVar(&flushInt, "flush", time.Minute, "max amount of time to wait before flushing")
	flag.BoolVar(&test, "test", false, "run as a client for the proxy in test mode")
	flag.Parse()

	if test {
		if err := testClient(laddr); err != nil { panic(err) }
	} else {
		proxy, err := NewInfluxDBProxy(laddr, raddr, flushInt)
		if err != nil { panic(err) }
		log.Fatal(proxy.Listen())
	}
}

func testClient(laddr string) error {
	serverAddr, err := net.ResolveUDPAddr("udp", laddr)
	if err != nil {
		return err
	}
	udpConn, err := net.DialUDP("udp", nil, serverAddr)
	if err != nil {
		return err
	}
	if udpConn == nil {
		return errors.New("UDP isn't enabled. Make sure to set config.IsUDP to true")
	}

	for {
		series := []*influxdb.Series{
			&influxdb.Series{
				Name: "jigish.test",
				Columns: []string{"time", "value"},
				Points:  [][]interface{}{[]interface{}{time.Now().Unix(), rand.Intn(100)}},
			},
			&influxdb.Series{
				Name: "jigish.test2",
				Columns: []string{"time", "value"},
				Points:  [][]interface{}{[]interface{}{time.Now().Unix(), rand.Intn(100)}},
			},
		}
		data, err := json.Marshal(series)
		if err != nil {
			return err
		}
		if len(data) >= UDPMaxMessageSize {
			err = fmt.Errorf("data size over limit %v limit is %v", len(data), UDPMaxMessageSize)
			log.Println(err.Error())
			return err
		}
		_, err = udpConn.Write(data)
		if err != nil {
			return err
		}
	}
	return nil
}

type InfluxDBProxy struct {
	laddr    *net.UDPAddr
	raddr    *net.UDPAddr
	lconn    *net.UDPConn
	rconn    *net.UDPConn
	buf      *bytes.Buffer
	bufN     int
	buflock  *sync.Mutex
	flushInt time.Duration
}

func NewInfluxDBProxy(laddr, raddr string, flushInt time.Duration) (*InfluxDBProxy, error) {
	l, err := net.ResolveUDPAddr("udp", laddr)
	if err != nil { return nil, err }
	r, err := net.ResolveUDPAddr("udp", raddr)
	if err != nil { return nil, err }
	return &InfluxDBProxy{
		laddr:    l,
		raddr:    r,
		buf:      bytes.NewBuffer(make([]byte, 0, UDPMaxMessageSize)),
		bufN:     0,
		buflock:  &sync.Mutex{},
		flushInt: flushInt,
	}, nil
}

func (p *InfluxDBProxy) Listen() (err error) {
	p.rconn, err = net.DialUDP("udp", nil, p.raddr)
	if err != nil { return err }
	defer p.closeRConn() // don't directly close because rconn object can change
	p.lconn, err = net.ListenUDP("udp", p.laddr)
	if err != nil { return err }
	defer p.lconn.Close()

	go p.flushOnInterval()
	log.Println("Proxying from "+p.laddr.String()+" to "+p.raddr.String())
	buffer := make([]byte, UDPMaxMessageSize)
	for {
		n, _, err := p.lconn.ReadFromUDP(buffer)
		if err != nil || n == 0 {
			log.Printf("UDP ReadFromUDP error: %s", err)
			continue
		} else if n <= 4 {
			// "[{}]" is 4 characters and we don't care about empty stuff so ignore it
			continue
		}

		series := []*influxdb.Series{}
		decoder := json.NewDecoder(bytes.NewBuffer(buffer[0:n]))
		err = decoder.Decode(&series)
		if err != nil {
			log.Printf("json unmarshal error: %s", err)
			continue
		}

		// now that the series was deserialized properly, lets try to append it to our buffer
		for _, singleSeries := range series {
			p.BufferSeriesOrFlush(singleSeries)
		}
	}
}

func (p *InfluxDBProxy) BufferSeriesOrFlush(series *influxdb.Series) {
	seriesBytes, err := json.Marshal(series)
	if err != nil {
		// eh, just skip the series
		log.Printf("json marshal error: %s", err)
		return
	}
	if len(seriesBytes) <= 2 {
		// nothing to write
		return
	}
	p.buflock.Lock() // lock here to make sure p.buf size doesn't change on us
	if len(seriesBytes) + 2 + p.bufN > UDPMaxMessageSize {
		if p.bufN == 0 {
			// no way we can fit this series, skip it
			log.Printf("impossible series with size %d", len(seriesBytes))
			return
		} else {
			// series is too big to fit this time, close the array and flush
			log.Println("Flush for size")
			p.Flush()
		}
	}
	if p.bufN == 0 {
		p.buf.WriteByte(byte(91)) // "["
	} else {
		p.buf.WriteByte(byte(44)) // ","
	}
	p.bufN++
	n, _ := p.buf.Write(seriesBytes)
	p.bufN += n
	p.buflock.Unlock()
}

// must be surrounded by lock/unlock!
func (p *InfluxDBProxy) Flush() {
	if p.bufN == 0 { return } // don't flush if we haven't written anything...
	p.buf.WriteByte(byte(93)) // close array with "]"
	p.bufN++
	log.Println("-> Flushing")
	_, err := p.rconn.Write(p.buf.Bytes())
	if err != nil {
		log.Printf("error writing udp. retrying...")
		// likely rconn was closed, lets try to reopen
		p.rconn.Close() // force close just in case
		p.rconn, err = net.DialUDP("udp", nil, p.raddr)
		if err == nil {
			_, err := p.rconn.Write(p.buf.Bytes()) // lets try this again, if it fails w/e
			if err != nil {
				log.Printf("-> retry write failed")
			}
		} else {
			log.Printf("-> retry conn failed")
		}
	}
	p.buf.Truncate(0)
	p.bufN = 0
}

func (p *InfluxDBProxy) flushOnInterval() {
	for {
		time.Sleep(p.flushInt)
		p.buflock.Lock()
		log.Println("Flush for time")
		p.Flush()
		p.buflock.Unlock()
	}
}

func (p *InfluxDBProxy) closeRConn() {
	if p.rconn == nil { return }
	p.rconn.Close()
}
