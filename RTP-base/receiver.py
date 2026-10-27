import argparse
import socket
import sys

from utils import PacketHeader, compute_checksum


def make_ack(seq_num):
    """Tạo gói tin ACK lũy kế (cumulative ACK) với checksum."""
    pkt_header = PacketHeader(type=3, seq_num=seq_num, length=0)
    pkt_header.checksum = compute_checksum(pkt_header / b"")
    return bytes(pkt_header / b"")


def parse_pkt(raw_bytes):
    """Giải mã gói tin nhận được và kiểm tra checksum."""
    if len(raw_bytes) < 16:
        return None, b""
    pkt = PacketHeader(raw_bytes)
    chk = pkt.checksum
    pkt.checksum = 0
    if compute_checksum(pkt) != chk:
        return None, b""  # Checksum sai -> Bỏ gói tin
    payload = bytes(pkt.payload)
    return pkt, payload


def receiver(receiver_ip, receiver_port, window_size):
    # Mở UDP socket và bind vào port nhận
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind((receiver_ip, receiver_port))

    expected_seq = 1
    buffer = {}
    connected = False

    while True:
        raw_bytes, sender_addr = s.recvfrom(2048)
        pkt, payload = parse_pkt(raw_bytes)

        if not pkt:
            continue  # Bỏ qua gói hỏng checksum

        # 1. Xử lý gói START
        if pkt.type == 0:
            if not connected:
                connected = True
            s.sendto(make_ack(1), sender_addr)
            continue

        if not connected:
            continue

        # 2. Xử lý gói DATA
        if pkt.type == 2:
            seq = pkt.seq_num
            # Kiểm tra gói nằm trong phạm vi cửa sổ [expected_seq, expected_seq + window_size)
            if seq < expected_seq + window_size:
                if seq >= expected_seq:
                    buffer[seq] = payload

                # Ghi ra stdout các gói in-order liên tiếp
                while expected_seq in buffer:
                    sys.stdout.buffer.write(buffer[expected_seq])
                    sys.stdout.buffer.flush()
                    del buffer[expected_seq]
                    expected_seq += 1

                # Gửi cumulative ACK cho seq_num kế tiếp đang kỳ vọng
                s.sendto(make_ack(expected_seq), sender_addr)

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