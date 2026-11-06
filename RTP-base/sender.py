import argparse
import socket
import select
import sys
import time

from utils import PacketHeader, compute_checksum


def make_pkt(pkt_type, seq_num, data=b""):
    pkt_header = PacketHeader(type=pkt_type, seq_num=seq_num, length=len(data))
    pkt_header.checksum = compute_checksum(pkt_header / data)
    return bytes(pkt_header / data)


def parse_pkt(raw_bytes):
    if len(raw_bytes) < 16:
        return None, b""
    pkt = PacketHeader(raw_bytes)
    chk = pkt.checksum
    pkt.checksum = 0
    if compute_checksum(pkt) != chk:
        return None, b""
    payload = bytes(pkt.payload)
    return pkt, payload


def sender(receiver_ip, receiver_port, window_size):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setblocking(False)
    addr = (receiver_ip, receiver_port)

    # 1. START Handshake
    start_pkt = make_pkt(0, 0)
    s.sendto(start_pkt, addr)

    while True:
        ready = select.select([s], [], [], 0.5)
        if ready[0]:
            try:
                data, _ = s.recvfrom(2048)
                pkt, _ = parse_pkt(data)
                if pkt and pkt.type == 3 and pkt.seq_num == 1:
                    break
            except socket.error:
                pass
        s.sendto(start_pkt, addr)

    # 2. Đọc sys.stdin
    raw_data = sys.stdin.buffer.read()
    CHUNK_SIZE = 1456
    chunks = (
        [raw_data[i : i + CHUNK_SIZE] for i in range(0, len(raw_data), CHUNK_SIZE)]
        if raw_data
        else [b""]
    )

    packets = [make_pkt(2, i + 1, chunks[i]) for i in range(len(chunks))]
    total_packets = len(packets)

    send_base = 1
    next_seq_num = 1
    timer_start = None

    # 3. Vòng lặp Go-Back-N
    while send_base <= total_packets:
        while next_seq_num < send_base + window_size and next_seq_num <= total_packets:
            s.sendto(packets[next_seq_num - 1], addr)
            if send_base == next_seq_num:
                timer_start = time.time()
            next_seq_num += 1

        ready = select.select([s], [], [], 0.05)
        if ready[0]:
            try:
                recv_bytes, _ = s.recvfrom(2048)
                pkt, _ = parse_pkt(recv_bytes)
                if pkt and pkt.type == 3:
                    if pkt.seq_num > send_base:
                        send_base = pkt.seq_num
                        if send_base < next_seq_num:
                            timer_start = time.time()
                        else:
                            timer_start = None
            except socket.error:
                pass

        if timer_start and (time.time() - timer_start >= 0.5):
            for i in range(send_base, next_seq_num):
                s.sendto(packets[i - 1], addr)
            timer_start = time.time()

    # 4. END Handshake
    end_seq = total_packets + 1
    end_pkt = make_pkt(1, end_seq)
    s.sendto(end_pkt, addr)

    end_timer = time.time()
    while time.time() - end_timer < 0.5:
        ready = select.select([s], [], [], 0.05)
        if ready[0]:
            try:
                recv_bytes, _ = s.recvfrom(2048)
                pkt, _ = parse_pkt(recv_bytes)
                if pkt and pkt.type == 3 and pkt.seq_num >= end_seq:
                    break
            except socket.error:
                pass

    s.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("receiver_ip")
    parser.add_argument("receiver_port", type=int)
    parser.add_argument("window_size", type=int)
    args = parser.parse_args()

    sender(args.receiver_ip, args.receiver_port, args.window_size)


if __name__ == "__main__":
    main()