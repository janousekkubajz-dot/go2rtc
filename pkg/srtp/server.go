package srtp

import (
	"encoding/binary"
	"net"
	"strconv"
	"sync"
)

type Server struct {
	address  string
	conn     net.PacketConn
	sessions map[uint32]*Session
	mu       sync.Mutex
}

func NewServer(address string) *Server {
	return &Server{
		address:  address,
		sessions: map[uint32]*Session{},
	}
}

func (s *Server) Port() int {
	if s.conn != nil {
		return s.conn.LocalAddr().(*net.UDPAddr).Port
	}
	_, a, _ := net.SplitHostPort(s.address)
	i, _ := strconv.Atoi(a)
	return i
}

func (s *Server) AddSession(session *Session) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := session.init(); err != nil {
		return
	}
	if len(s.sessions) == 0 {
		var err error
		if s.conn, err = net.ListenPacket("udp", s.address); err != nil {
			session.reorder.Close()
			return
		}
		if udpConn, ok := s.conn.(*net.UDPConn); ok {
			_ = udpConn.SetReadBuffer(4 << 20)
		}
		go s.handle(s.conn)
	}

	session.conn = s.conn
	s.sessions[session.Remote.SSRC] = session
}

func (s *Server) DelSession(session *Session) {
	s.mu.Lock()
	delete(s.sessions, session.Remote.SSRC)

	if len(s.sessions) == 0 && s.conn != nil {
		_ = s.conn.Close()
		s.conn = nil
	}
	s.mu.Unlock()

	if session.reorder != nil {
		session.reorder.Close()
	}
}

func (s *Server) GetSession(ssrc uint32) (session *Session) {
	s.mu.Lock()
	session = s.sessions[ssrc]
	s.mu.Unlock()
	return
}

func (s *Server) handle(conn net.PacketConn) error {
	packets := make(chan []byte, 1024)
	go func() {
		for b := range packets {
			if len(b) < 2 {
				continue
			}
			switch packetType := b[1]; packetType {
			case 99, 110, 0x80 | 99, 0x80 | 110:
				if len(b) < 12 {
					continue
				}
				ssrc := binary.BigEndian.Uint32(b[8:])
				if session := s.GetSession(ssrc); session != nil {
					session.ReadRTP(b)
				}
			case 200, 201, 202, 203, 204, 205, 206, 207:
				if len(b) < 8 {
					continue
				}
				ssrc := binary.BigEndian.Uint32(b[4:])
				if session := s.GetSession(ssrc); session != nil {
					session.ReadRTCP(b)
				}
			}
		}
	}()

	b := make([]byte, 2048)
	defer close(packets)
	for {
		n, _, err := conn.ReadFrom(b)
		if err != nil {
			return err
		}
		if n < 2 {
			continue
		}

		packet := make([]byte, n)
		copy(packet, b[:n])
		select {
		case packets <- packet:
		default:
			// Never stall the UDP reader; discard only on queue overflow.
		}
	}
}
