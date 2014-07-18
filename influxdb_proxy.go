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
	"strings"
	"sync"
	"time"
)

const UDPMaxMessageSize = 2048 // max bytes we can send to InfluxDB.
                               // see: https://github.com/influxdb/influxdb/blob/master/api/udp/api.go

func main() {
	var (
		laddr     string
		raddr     string
		flushInt  time.Duration
		test      bool
		testSleep time.Duration
	)
	flag.StringVar(&laddr, "laddr", ":2004", "local address to bind to")
	flag.StringVar(&raddr, "raddr", ":5551", "remote address to proxy to")
	flag.DurationVar(&flushInt, "flush", 10*time.Second, "max amount of time to wait before flushing")
	flag.BoolVar(&test, "test", false, "run as a client for the proxy in test mode")
	flag.DurationVar(&testSleep, "test-sleep", 250*time.Millisecond, "how long to sleep between test series send")
	flag.Parse()

	if test {
		if err := testClient(laddr, testSleep); err != nil { panic(err) }
	} else {
		proxy, err := NewInfluxDBProxy(laddr, raddr, flushInt)
		if err != nil { panic(err) }
		log.Fatal(proxy.Listen())
	}
}

func testClient(laddr string, sleep time.Duration) error {
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
		time.Sleep(sleep)
		series := []*influxdb.Series{
			&influxdb.Series{
				Name: "influxdb-proxy.test",
				Columns: []string{"time", "value"},
				Points:  [][]interface{}{
					[]interface{}{time.Now().Unix(), rand.Intn(100)},
					[]interface{}{time.Now().Unix(), rand.Intn(100)},
					[]interface{}{time.Now().Unix(), rand.Intn(100)},
					[]interface{}{time.Now().Unix(), rand.Intn(100)},
				},
			},
			&influxdb.Series{
				Name: "influxdb-proxy.test2",
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
		log.Println("Sending: "+string(data))
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
	bufIdx   map[string]int // name -> series
	buf      []*influxdb.Series
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
		bufIdx:   map[string]int{},
		buf:      []*influxdb.Series{},
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
			p.BufferSeries(singleSeries)
		}
	}
}

// assume columns are the same. fair assumption.
func mergeSeries(dst, src *influxdb.Series) {
	dst.Points = append(dst.Points, src.Points...)
}

// we make the assumption that no single series will be > 2048 bytes serialized. this is fair because our
// UDP buffer size is 2048 bytes.
func (p *InfluxDBProxy) BufferSeries(series *influxdb.Series) {
	// add series to buffer map
	p.buflock.Lock()
	normalizedIdx, exists := p.bufIdx[series.Name]
	if !exists {
		p.bufIdx[series.Name] = len(p.buf)
		p.buf = append(p.buf, series)
	} else {
		normalizedSeries := p.buf[normalizedIdx]
		mergeSeries(normalizedSeries, series)
	}
	p.buflock.Unlock()
}

func (p *InfluxDBProxy) send(buf *bytes.Buffer) {
	log.Println("-> sending")
	_, err := p.rconn.Write(buf.Bytes())
	if err != nil {
		log.Printf("-> error sending udp. retrying...")
		// likely rconn was closed, lets try to reopen
		p.rconn.Close() // force close just in case
		p.rconn, err = net.DialUDP("udp", nil, p.raddr)
		if err == nil {
			_, err := p.rconn.Write(buf.Bytes()) // lets try this again, if it fails w/e
			if err != nil {
				log.Printf("-> retry send failed")
			}
		} else {
			log.Printf("-> retry conn failed")
		}
	}
}

func (p *InfluxDBProxy) splitAndSend(series *influxdb.Series) {
	// hand-craft the message.
	prefix := []byte("[{\"name\":\""+series.Name+"\",\"columns\":[\""+strings.Join(series.Columns, "\",\"")+"\"],\"points\":[")
	suffix := []byte("]}]")
	if len(prefix)+len(suffix) >= UDPMaxMessageSize {
		log.Println("impossible series with large metadata ("+series.Name+"). skipping.")
	}
	buf := bytes.NewBuffer(make([]byte, 0, UDPMaxMessageSize))
	for _, point := range series.Points {
		pointBytes, err := json.Marshal(point)
		if err != nil {
			log.Println("json point marshal error: "+err.Error())
			continue
		}
		if buf.Len() == 0 {
			buf.Write(prefix)
		}
		// have to add 1 for the comma thay we may have to add.
		if len(pointBytes) + buf.Len() + 1 + len(suffix) <= UDPMaxMessageSize {
			// we're within bounds
			if buf.Len() > len(prefix) { // we already have a point in the buffer
				buf.WriteByte(byte(44)) // ","
			}
			buf.Write(pointBytes)
		} else if buf.Len() == len(prefix) {
			// buf is full but this means the point itself is too big so lets skip it.
			log.Println("impossible point. skipping.")
			continue
		} else {
			// buf is full. we have data, so lets send it and then add the point.
			buf.Write(suffix)
			p.send(buf)
			buf.Truncate(0)
			buf.Write(prefix)
			// no +1 because there is no comma needed yet.
			if len(pointBytes) + buf.Len() + len(suffix) <= UDPMaxMessageSize {
				buf.Write(pointBytes)
				continue
			}
			// again, the point is too big.
			log.Println("impossible point. skipping.")
		}
	}
}

func (p *InfluxDBProxy) Flush() {
	p.buflock.Lock()
	if len(p.buf) == 0 {
		log.Println("-> nothing to flush")
		p.buflock.Unlock()
		return
	}
	bufArr := p.buf
	p.buf = []*influxdb.Series{}
	p.bufIdx = map[string]int{}
	p.buflock.Unlock()

	buf := bytes.NewBuffer(make([]byte, 0, UDPMaxMessageSize))
	for _, series := range bufArr {
		seriesBytes, err := json.Marshal(series)
		if err != nil {
			log.Println("json series marshal error: "+err.Error())
			continue
		}
		if (len(seriesBytes) + buf.Len() + 2) <= UDPMaxMessageSize {
			// we're within the bounds, add it and move on
			if buf.Len() == 0 {
				buf.WriteByte(byte(91)) // begin array with "["
			} else {
				buf.WriteByte(byte(44)) // ","
			}
			buf.Write(seriesBytes)
			continue
		}
		// buf is full, lets figure out what to do with it
		if buf.Len() > 0 {
			// there is data in the buffer, lets send it off.
			buf.WriteByte(byte(93)) // end array with "]"
			p.send(buf)
			buf.Truncate(0)
			if len(seriesBytes) + 2 <= UDPMaxMessageSize {
				// we're within the bounds, add it and move on
				buf.WriteByte(byte(91)) // begin array with "["
				buf.Write(seriesBytes)
				continue
			} else {
				// sucky case. our series is too long to fit in a single message.
				p.splitAndSend(series)
			}
		} else {
			// sucky case. our series is too long to fit in a single message.
			p.splitAndSend(series)
		}
	}
	if buf.Len() > 0 {
		buf.WriteByte(byte(93)) // end array with "]"
		p.send(buf)
	}
}

func (p *InfluxDBProxy) flushOnInterval() {
	for {
		time.Sleep(p.flushInt)
		log.Println("flush")
		p.Flush()
	}
}

func (p *InfluxDBProxy) closeRConn() {
	if p.rconn == nil { return }
	p.rconn.Close()
}
