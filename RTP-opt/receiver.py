import argparse
import socket
import sys

from utils import PacketHeader, compute_checksum


def make_ack(seq_num):
    """Tạo gói ACK cá thể (Individual ACK với seq_num bằng đúng gói nhận được)."""
    pkt_header = PacketHeader(type=3, seq_num=seq_num, length=0)
    pkt_header.checksum = compute_checksum(pkt_header / b"")
    return bytes(pkt_header / b"")


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


def receiver(receiver_ip, receiver_port, window_size):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind((receiver_ip, receiver_port))

    expected_seq = 1
    buffer = {}
    connected = False

    while True:
        raw_bytes, sender_addr = s.recvfrom(2048)
        pkt, payload = parse_pkt(raw_bytes)

        if not pkt:
            continue

        # 1. Xử lý gói START
        if pkt.type == 0:
            if not connected:
                connected = True
            s.sendto(make_ack(1), sender_addr)
            continue

        if not connected:
            continue

        # 2. Xử lý gói DATA (Individual ACK)
        if pkt.type == 2:
            seq = pkt.seq_num
            # Gửi ACK riêng cho chính gói tin vừa nhận được
            if seq < expected_seq + window_size:
                s.sendto(make_ack(seq), sender_addr)
                if seq >= expected_seq:
                    buffer[seq] = payload

                # Xuất dữ liệu in-order liên tiếp ra stdout
                while expected_seq in buffer:
                    sys.stdout.buffer.write(buffer[expected_seq])
                    sys.stdout.buffer.flush()
                    del buffer[expected_seq]
                    expected_seq += 1

        # 3. Xử lý gói END
        elif pkt.type == 1:
            s.sendto(make_ack(pkt.seq_num), sender_addr)
            break

    s.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "receiver_ip", help="The IP address of the host that receiver is running on"
    )
    parser.add_argument(
        "receiver_port", type=int, help="The port number on which receiver is listening"
    )
    parser.add_argument(
        "window_size", type=int, help="Maximum number of outstanding packets"
    )
    args = parser.parse_args()

    receiver(args.receiver_ip, args.receiver_port, args.window_size)


if __name__ == "__main__":
    main()