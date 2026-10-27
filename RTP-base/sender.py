import argparse
import socket
import select
import sys
import time

from utils import PacketHeader, compute_checksum


def make_pkt(pkt_type, seq_num, data=b""):
    """Tạo gói tin RTP kết hợp Header và Payload, tính sẵn Checksum."""
    pkt_header = PacketHeader(type=pkt_type, seq_num=seq_num, length=len(data))
    pkt_header.checksum = compute_checksum(pkt_header / data)
    return bytes(pkt_header / data)


def parse_pkt(raw_bytes):
    """Giải mã gói tin nhận được và kiểm tra tính toàn vẹn Checksum."""
    if len(raw_bytes) < 16:
        return None, b""
    pkt = PacketHeader(raw_bytes)
    chk = pkt.checksum
    pkt.checksum = 0
    if compute_checksum(pkt) != chk:
        return None, b""  # Checksum sai -> Bỏ gói tin
    payload = bytes(pkt.payload)
    return pkt, payload


def sender(receiver_ip, receiver_port, window_size):
    # Khởi tạo UDP Socket ở chế độ non-blocking
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setblocking(False)
    addr = (receiver_ip, receiver_port)

    # -------------------------------------------------------------
    # BƯỚC 1: Khởi tạo kết nối (START Handshake)
    # -------------------------------------------------------------
    start_pkt = make_pkt(0, 0)
    s.sendto(start_pkt, addr)

    # Chờ ACK(1) cho gói START
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
        # Retransmit START nếu quá timeout 500ms
        s.sendto(start_pkt, addr)

    # -------------------------------------------------------------
    # BƯỚC 2: Đọc dữ liệu từ sys.stdin & Chia nhỏ thành gói DATA
    # -------------------------------------------------------------
    raw_data = sys.stdin.buffer.read()
    CHUNK_SIZE = 1456
    chunks = (
        [raw_data[i : i + CHUNK_SIZE] for i in range(0, len(raw_data), CHUNK_SIZE)]
        if raw_data
        else [b""]
    )

    # seq_num của DATA bắt đầu từ 1
    packets = [make_pkt(2, i + 1, chunks[i]) for i in range(len(chunks))]
    total_packets = len(packets)

    send_base = 1
    next_seq_num = 1
    timer_start = None

    # -------------------------------------------------------------
    # BƯỚC 3: Truyền dữ liệu với Cửa sổ trượt (Go-Back-N)
    # -------------------------------------------------------------
    while send_base <= total_packets:
        # Gửi thêm gói mới nếu cửa sổ trượt còn chỗ trống
        while next_seq_num < send_base + window_size and next_seq_num <= total_packets:
            s.sendto(packets[next_seq_num - 1], addr)
            if send_base == next_seq_num:
                timer_start = time.time()  # Bật timer cho gói đầu cửa sổ
            next_seq_num += 1

        # Nhận ACK từ Receiver
        ready = select.select([s], [], [], 0.05)
        if ready[0]:
            try:
                recv_bytes, _ = s.recvfrom(2048)
                pkt, _ = parse_pkt(recv_bytes)
                if pkt and pkt.type == 3:  # Loại ACK
                    if pkt.seq_num > send_base:
                        send_base = pkt.seq_num
                        if send_base < next_seq_num:
                            timer_start = time.time()  # Reset timer
                        else:
                            timer_start = None  # Đã ACK hết các gói hiện tại
            except socket.error:
                pass

        # Xử lý Timeout 500ms: Retransmit TOÀN BỘ các gói trong cửa sổ
        if timer_start and (time.time() - timer_start >= 0.5):
            for i in range(send_base, next_seq_num):
                s.sendto(packets[i - 1], addr)
            timer_start = time.time()

    # -------------------------------------------------------------
    # BƯỚC 4: Ngắt kết nối (END)
    # -------------------------------------------------------------
    end_seq = total_packets + 1
    end_pkt = make_pkt(1, end_seq)
    s.sendto(end_pkt, addr)

    # Chờ ACK của END hoặc thoát sau 500ms
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

    sender(args.receiver_ip, args.receiver_port, args.window_size)


if __name__ == "__main__":
    main()