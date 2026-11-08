# UC-01: Một broker bị down

| | |
|---|---|
| **Mức độ** | ⚠️ Warning: cluster vẫn chạy nhưng mất dự phòng. Chuyển thành 🔴 Critical nếu thêm broker nữa down |
| **Alert liên quan** | `KafkaBrokerDown`, `KafkaUnderReplicatedPartitions` |
| **Đã kiểm chứng trên lab** | 2026-11-08, stop `kafka2` (3 broker, RF=3, min ISR=2) |

---

## 1. Kiến thức nền (đọc lại khi quên)

```
Cluster
 └── Broker (máy chủ: lưu message, phục vụ đọc/ghi)
       └── Topic (luồng message có tên: orders, payments...)
             └── Partition (lát cắt của topic, để chạy song song)
                   └── Replica (bản sao của partition trên các broker khác nhau)
                         ├── 1 Leader    → nhận MỌI đọc/ghi
                         └── N Follower  → chép từ leader, chờ thay thế
```

- **Producer ghi vào broker. Consumer chủ động đọc (pull) từ broker.**
- **Replication factor (RF) là của partition, không phải của broker.** Cluster 10 broker với RF=3 thì mỗi partition nằm trên 3 trong 10 broker.
- **ISR (in-sync replicas)** = các replica đang theo kịp leader. Khỏe là khi ISR = toàn bộ replicas.
- **Under-replicated partition** = partition có ISR < số replica, tức là có follower không theo kịp hoặc đã chết.
- **`min.insync.replicas=2`**: với `acks=all`, một lần ghi phải được ít nhất 2 replica in-sync xác nhận, nếu không sẽ bị từ chối.

---

## 2. Triệu chứng: nhìn ở đâu, thấy gì

Số liệu thật đo trên lab khi stop `kafka2`:

| Chỉ số (Grafana → Kafka Overview) | Bình thường | Khi kafka2 down | Ý nghĩa |
|---|---|---|---|
| Brokers online | 3 | **2** | Mất 1 broker |
| Under-replicated partitions | 0 | **62** (toàn bộ) | RF=3 trên 3 broker nên partition nào cũng có 1 replica trên kafka2 |
| Offline partitions | 0 | **0** | ✅ Không partition nào mất leader, nên chưa phải outage |
| Under min ISR | 0 | **0** | ✅ Mỗi partition còn 2 replica ≥ min ISR 2, ghi vẫn OK |
| Partition leaders per broker | ~20 / 20 / 20 | kafka1=30, kafka3=32, kafka2 mất | Leader của kafka2 đã dồn sang 2 broker còn lại |
| Messages in/sec (orders) | 200 | **193 → 200** | Chỉ khựng nhẹ vài giây |
| Consumer lag | ~330 | ~335 | Consumer không bị ảnh hưởng |

**CLI:**
```bash
bin/kafka kafka-topics.sh --describe --under-replicated-partitions
bin/kafka kafka-topics.sh --describe --topic orders
```
```
Partition: 0  Leader: 3  Replicas: 2,3,1  Isr: 3,1     ← leader cũ là 2, giờ là 3; ISR thiếu 2
Partition: 2  Leader: 1  Replicas: 1,2,3  Isr: 1,3     ← leader không đổi, chỉ mất follower 2
```

**Log producer** (`docker compose logs producer`): thấy vài dòng WARN như sau, rồi tự hết:
```
WARN ... Got error produce response ... on topic-partition orders-3, retrying ... Error: NOT_LEADER_OR_FOLLOWER
```
👉 Đây là **bình thường**: producer gửi tới leader cũ, bị báo "tôi không còn là leader", nên lấy lại metadata và gửi sang leader mới. Không mất message, vì `acks=all` và producer tự retry.

**Alert (Prometheus → /alerts):** `KafkaBrokerDown` và `KafkaUnderReplicatedPartitions` chuyển *pending → firing* sau khoảng 30 giây đến 1 phút.

---

## 3. Kafka tự làm gì (không cần can thiệp)

1. Controller phát hiện broker mất kết nối.
2. Với mỗi partition có **leader trên broker chết**: bầu leader mới từ ISR. Mất khoảng vài giây.
3. Với partition chỉ có **follower trên broker chết**: loại broker đó khỏi ISR, leader giữ nguyên.
4. Client (producer/consumer) tự refresh metadata và chuyển sang leader mới.

> ⚠️ **Kafka KHÔNG tự tạo bản sao thứ 3 trên broker khác.** Nó chạy với 2/3 bản và **chờ broker cũ quay lại**. Lý do: tránh copy hàng trăm GB mỗi lần broker restart vài phút.

---

## 4. Khôi phục: chọn theo tình huống

### Bước 0: Chẩn đoán vì sao broker down
```bash
docker compose ps kafka2                 # Prod: systemctl status kafka / kubectl get pod
docker compose logs --tail 100 kafka2    # Tìm: OutOfMemoryError, No space left on device, IOException
```
Kiểm tra: RAM/heap (OOM), **dung lượng đĩa** (nguyên nhân hay gặp nhất), mạng, máy chủ có còn sống không.

### Trường hợp A: Broker restart hoặc mất mạng tạm, đĩa còn nguyên ✅ (phổ biến nhất)
```bash
docker compose start kafka2
```
Broker tự kéo phần dữ liệu thiếu từ leader và tự vào lại ISR. **Trên lab:**

| Thời điểm | Trạng thái |
|---|---|
| t+15s | kafka2 healthy, URP = 32 (đang bắt kịp) |
| t+25s | **URP = 0**, ISR đủ `3,1,2` |
| lúc này | kafka2 **lead 0 partition** (kafka1=30, kafka3=32) ← cần bước C |

Thời gian bắt kịp tỉ lệ với lượng dữ liệu ghi trong lúc broker down. Ở production có thể mất vài phút đến vài giờ.

### Trường hợp B: Broker hỏng hẳn và mất đĩa
- Dựng máy mới, **dùng lại đúng `node.id` cũ** (ví dụ `node.id=2`), cùng `cluster.id`, và format storage bằng `kafka-storage.sh format`.
- Broker mới thấy mình trống nên tự copy lại toàn bộ replica của nó từ các leader.
- ⚠️ Việc này sinh rất nhiều traffic mạng. Nên đặt giới hạn băng thông replication (`leader.replication.throttled.rate` / `follower.replication.throttled.rate`).

### Trường hợp C: Muốn bỏ hẳn broker, chuyển replica sang broker khác
```bash
# 1. Tạo plan di chuyển
echo '{"topics":[{"topic":"orders"}],"version":1}' > topics.json
bin/kafka kafka-reassign-partitions.sh --topics-to-move-json-file topics.json --broker-list "1,3,4" --generate
# 2. Lưu plan vào reassign.json, rồi chạy (nên có --throttle)
bin/kafka kafka-reassign-partitions.sh --reassignment-json-file reassign.json --execute --throttle 50000000
# 3. Kiểm tra tiến độ
bin/kafka kafka-reassign-partitions.sh --reassignment-json-file reassign.json --verify
```
Ở quy mô lớn, dùng **Cruise Control** để tự tính plan và cân bằng.

> Lưu ý: các file JSON phải nằm *bên trong* container `tools`. Trên lab chỉ đọc hiểu, chưa thực hành phần này.

### Bước cuối (mọi trường hợp): trả leader về cho cân bằng
```bash
bin/kafka kafka-leader-election.sh --election-type preferred --all-topic-partitions
```
**Trên lab:** leader từ `0 / 30 / 32` về **`21 / 20 / 21`**. Nếu không chạy lệnh này, Kafka tự làm sau khoảng 5 phút (`auto.leader.rebalance.enable=true`, `leader.imbalance.check.interval.seconds=300`).

---

## 5. Checklist xác nhận đã khôi phục

- [ ] Brokers online = số broker mong đợi
- [ ] Under-replicated partitions = **0**
- [ ] Offline partitions = **0**, Under min ISR = **0**
- [ ] Leader chia đều giữa các broker
- [ ] Messages in/sec trở về mức bình thường
- [ ] Consumer lag phẳng, không tăng dần
- [ ] Alert `KafkaBrokerDown` và `KafkaUnderReplicatedPartitions` hết firing
- [ ] Đã ghi nguyên nhân gốc (OOM? đĩa đầy? bảo trì?) và hướng phòng tránh

---

## 6. Khi nào thì nghiêm trọng hơn?

Với RF=3, `min.insync.replicas=2`, `acks=all`:

| Số replica còn sống của một partition | Đọc | Ghi | Trạng thái |
|---|---|---|---|
| 3/3 | ✅ | ✅ | Khỏe |
| 2/3 | ✅ | ✅ | Under-replicated (⚠️ **tình huống này**) |
| 1/3 | ✅ | ❌ `NOT_ENOUGH_REPLICAS` | Under min ISR (🔴) |
| 0/3 | ❌ | ❌ | Offline (🔴 outage) |

- Cluster 10 broker mà 2 broker chết: **chỉ các partition có replica nằm trên cả 2 broker đó** bị chặn ghi.
- **KRaft controller quorum** (3 controller) cần đa số, tức 2/3, còn sống. Mất 2 controller thì cluster không thay đổi được metadata (không bầu leader mới, không shrink ISR được). Lab chạy combined mode nên mất 2 broker = mất 2 controller.
- **Phòng ngừa:** bật `broker.rack` (rack awareness) để 3 replica nằm ở 3 rack hoặc AZ khác nhau. Ở production nên tách controller riêng khỏi broker.

---

## 7. Bài học rút ra

1. Một broker down **không phải outage** nếu RF=3 và min ISR=2. Đó là lý do chọn cấu hình này.
2. **Offline partitions** và **Under min ISR** mới là hai chỉ số cần báo động khẩn cấp. Under-replicated chỉ là cảnh báo.
3. WARN `NOT_LEADER_OR_FOLLOWER` ở client lúc failover là bình thường. Chỉ lo nếu nó kéo dài.
4. Sau khi broker quay lại, **nhớ cân bằng leader**, nếu không broker đó ngồi không còn các broker khác chịu tải nhiều hơn.
5. Kafka không tự re-replicate sang broker khác. Broker chết hẳn thì phải **thay bằng broker cùng ID** hoặc **reassign partition**.

---

## 8. Tự diễn tập lại trên lab

```bash
docker compose stop kafka2        # quan sát Grafana + http://localhost:9090/alerts khoảng 1 phút
docker compose start kafka2       # xem URP về 0 sau khoảng 25 giây
bin/kafka kafka-leader-election.sh --election-type preferred --all-topic-partitions
```
