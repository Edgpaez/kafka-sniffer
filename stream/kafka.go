package stream

import (
	"bufio"
	"fmt"
	"io"
	"log"

	"github.com/Edgpaez/kafka-sniffer/kafka"
	"github.com/Edgpaez/kafka-sniffer/metrics"

	"github.com/google/gopacket"
	"github.com/google/gopacket/tcpassembly"
	"github.com/google/gopacket/tcpassembly/tcpreader"
)

// KafkaStreamFactory implements tcpassembly.StreamFactory
type KafkaStreamFactory struct {
	metricsStorage *metrics.Storage
	verbose        bool
}

// NewKafkaStreamFactory assembles streams
func NewKafkaStreamFactory(metricsStorage *metrics.Storage, verbose bool) *KafkaStreamFactory {
	return &KafkaStreamFactory{metricsStorage: metricsStorage, verbose: verbose}
}

// New assembles new stream
func (h *KafkaStreamFactory) New(net, transport gopacket.Flow) tcpassembly.Stream {
	s := &KafkaStream{
		net:            net,
		transport:      transport,
		r:              tcpreader.NewReaderStream(),
		metricsStorage: h.metricsStorage,
		verbose:        h.verbose,
	}

	go s.run() // Important... we must guarantee that data from the reader stream is read.

	return &s.r
}

// KafkaStream will handle the actual decoding of http requests.
type KafkaStream struct {
	net, transport gopacket.Flow
	r              tcpreader.ReaderStream
	metricsStorage *metrics.Storage
	verbose        bool
}

func (h *KafkaStream) run() {
	srcHost := fmt.Sprint(h.net.Src())
	srcPort := fmt.Sprint(h.transport.Src())
	dstHost := fmt.Sprint(h.net.Dst())
	dstPort := fmt.Sprint(h.transport.Dst())

	log.Printf("%s:%s -> %s:%s", srcHost, srcPort, dstHost, dstPort)
	log.Printf("%s:%s -> %s:%s", dstHost, dstPort, srcHost, srcPort)

	buf := bufio.NewReaderSize(&h.r, 2<<15) // 65k

	for {
		req, readBytes, err := kafka.DecodeRequest(buf)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return
		}

		if err != nil {
			log.Printf("unable to read request to Broker - skipping packet: %s\n", err)

			if _, ok := err.(kafka.PacketDecodingError); ok {
				_, err := buf.Discard(readBytes)
				if err != nil {
					log.Printf("could not discard: %s\n", err)
				}
			}

			continue
		}

		log.Printf("got request, key: %d, version: %d, correlationID: %d, clientID: %s\n", req.Key, req.Version, req.CorrelationID, req.ClientID)

		switch body := req.Body.(type) {
		case *kafka.ProduceRequest:
			for _, topic := range body.ExtractTopics() {
				log.Printf("client %s:%s wrote to topic %s", srcHost, srcPort, topic)
			}
		case *kafka.FetchRequest:
			for _, topic := range body.ExtractTopics() {
				log.Printf("client %s:%s read from topic %s", h.net.Src(), h.transport.Src(), topic)
			}
		case *kafka.OffsetFetchRequest:
			for _, topic := range body.Topics {
				log.Printf("client %s:%s joined group %s to read topic %s", h.net.Src(), h.transport.Src(), body.GroupId, topic.Name)
			}
		case *kafka.OffsetCommitRequest:
			for _, topic := range body.Topics {
				log.Printf("client %s:%s committed offset to group %s of topic %s", h.net.Src(), h.transport.Src(), body.GroupId, topic.Name)
			}
		}
	}
}
