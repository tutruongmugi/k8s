# Kafka Broker

# **Kafka 4.0 KRaft on Kubernetes – Setup Guide**

## **1. Kiến trúc tổng quan**

```text
📦 Namespace: kafka
 ┣━ 📜 ConfigMap: kafka-configmap         (Shared env vars)
 ┣━ 🏢 StatefulSet: kafka                 (3 replicas: broker + controller)
 ┃   ┣━ 🐳 Pod: kafka-0                   (BROKER_ID=10)
 ┃   ┣━ 🐳 Pod: kafka-1                   (BROKER_ID=20)
 ┃   ┗━ 🐳 Pod: kafka-2                   (BROKER_ID=30)
 ┣━ 🔌 Service: kafka                     (Headless - DNS nội bộ: kafka-{n}.kafka.kafka.svc.cluster.local)
 ┣━ 🌐 Service: kafka-external            (NodePort - Expose ra ngoài cluster)
 ┣━ 🛡️ PodDisruptionBudget: kafka-pdb     (Đảm bảo ít nhất 2 broker alive)
 ┗━ 📈 HPA: kafka-hpa                     (Scale dựa trên CPU - optional)
```

### **KRaft roles**

| **Pod** | **BROKER_ID** | **roles** |
| --- | --- | --- |
| kafka-0 | 10 | broker, controller |
| kafka-1 | 20 | broker, controller |
| kafka-2 | 30 | broker, controller |

Với 3 node chạy cả 2 roles thì không cần tách dedicated controller node. Nếu cluster lớn hơn, nên tách riêng.

---

## **2. Namespace**

```yaml
# namespace.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: kafka
  labels:
    name: kafka
```

---

## **3. ConfigMap**

ConfigMap chứa các biến môi trường dùng chung cho tất cả broker.

```yaml
# kafka-configmap.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: kafka-configmap
  namespace: kafka
data:
  # --- KRaft mode ---
  KAFKA_PROCESS_ROLES: "broker,controller"
  KAFKA_CONTROLLER_QUORUM_VOTERS: "10@kafka-0.kafka.kafka.svc.cluster.local:9093,20@kafka-1.kafka.kafka.svc.cluster.local:9093,30@kafka-2.kafka.kafka.svc.cluster.local:9093"

  # --- Listeners ---
  # INSIDE  : giao tiếp nội bộ cluster (broker <-> broker, client nội bộ)
  # OUTSIDE : client từ bên ngoài cluster kết nối qua NodePort
  # CONTROLLER: Raft metadata replication
  KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "INSIDE:PLAINTEXT,OUTSIDE:PLAINTEXT,CONTROLLER:PLAINTEXT"
  KAFKA_INTER_BROKER_LISTENER_NAME: "INSIDE"
  KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"

  # --- Log & Retention ---
  KAFKA_LOG_DIRS: "/kafka/data"
  KAFKA_LOG_RETENTION_HOURS: "168"
  KAFKA_LOG_SEGMENT_BYTES: "1073741824"
  KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS: "300000"

  # --- Performance ---
  KAFKA_NUM_NETWORK_THREADS: "6"
  KAFKA_NUM_IO_THREADS: "8"
  KAFKA_SOCKET_SEND_BUFFER_BYTES: "102400"
  KAFKA_SOCKET_RECEIVE_BUFFER_BYTES: "102400"
  KAFKA_SOCKET_REQUEST_MAX_BYTES: "104857600"

  # --- Replication ---
  KAFKA_DEFAULT_REPLICATION_FACTOR: "3"
  KAFKA_MIN_INSYNC_REPLICAS: "2"
  KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: "3"
  KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: "3"
  KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: "2"

  # --- Auto topic ---
  KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
  KAFKA_AUTO_LEADER_REBALANCE_ENABLE: "true"

  # --- JVM heap (tương ứng với 4000Mi request / 15000Mi limit) ---
  KAFKA_HEAP_OPTS: "-Xmx8g -Xms4g"
```

---

## **4. Headless Service & NodePort Service**

### **4.1 Headless Service (nội bộ)**

```yaml
# kafka-service-headless.yaml
apiVersion: v1
kind: Service
metadata:
  name: kafka
  namespace: kafka
  labels:
    app: kafka
spec:
  clusterIP: None  # Khai báo đây là Headless Service
  selector:
    app: kafka
  ports:
    - name: broker-inside
      port: 9092
      targetPort: 9092
    - name: controller
      port: 9093
      targetPort: 9093
    - name: jmx
      port: 1099
      targetPort: 1099
```

### **4.2 NodePort Service (external access)**

```yaml
# kafka-service-external.yaml
apiVersion: v1
kind: Service
metadata:
  name: kafka-external
  namespace: kafka
  labels:
    app: kafka
spec:
  type: NodePort
  selector:
    app: kafka
  ports:
    - name: broker-outside
      port: 9094
      targetPort: 9094
      nodePort: 30094  # chỉnh theo môi trường
```

---

## **5. StatefulSet**

> **Image**: `apache/kafka:4.0.0`
> 
> Official image từ Apache, hỗ trợ KRaft native.
> Entrypoint tự động format storage nếu `CLUSTER_ID` được set.

### **5.1 Tạo CLUSTER_ID (chạy 1 lần duy nhất)**

```bash
# Sinh CLUSTER_ID chuẩn base64 (22 ký tự) dùng cho Kafka KRaft
kubectl run kafka-init \
  --image=apache/kafka:4.0.0 \
  --restart=Never \
  --rm -it \
  -- /opt/kafka/bin/kafka-storage.sh random-uuid
```

Lưu giá trị output vào Secret:

```yaml
# kafka-cluster-id-secret.yaml
apiVersion: v1
kind: Secret
metadata:
  name: kafka-cluster-id
  namespace: kafka
type: Opaque
stringData:
  CLUSTER_ID: "<UUID-từ-lệnh-trên>"  # ví dụ: MkU3OEVBNTcwNTJENDM2Qg
```

### **5.2 ConfigMap server.properties & kraft-init.properties**

```yaml
# kafka-kraft-server-config.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: kafka-kraft-server-config
  namespace: kafka
data:
  server.properties: |
    # Được override bởi env vars, file này là fallback
    process.roles=broker,controller
    controller.listener.names=CONTROLLER
    inter.broker.listener.name=INSIDE
    listener.security.protocol.map=INSIDE:PLAINTEXT,OUTSIDE:PLAINTEXT,CONTROLLER:PLAINTEXT
    log.dirs=/kafka/data
    num.partitions=3
    default.replication.factor=3
    min.insync.replicas=2
    offsets.topic.replication.factor=3
    transaction.state.log.replication.factor=3
    transaction.state.log.min.isr=2
    log.retention.hours=168
    log.segment.bytes=1073741824
    log.retention.check.interval.ms=300000
    auto.create.topics.enable=true
```

```yaml
# kafka-kraft-init-config.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: kafka-kraft-init-config
  namespace: kafka
data:
  kraft-init.properties: |
    process.roles=broker,controller
    node.id=0
    log.dirs=/kafka/data
    controller.listener.names=CONTROLLER
    listener.security.protocol.map=CONTROLLER:PLAINTEXT
    controller.quorum.voters=10@kafka-0.kafka.kafka.svc.cluster.local:9093,20@kafka-1.kafka.kafka.svc.cluster.local:9093,30@kafka-2.kafka.kafka.svc.cluster.local:9093
```

### **5.3 StatefulSet manifest**

```yaml
# kafka-statefulset.yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: kafka
  namespace: kafka
spec:
  serviceName: "kafka" # phải trùng với tên Headless Service
  replicas: 3
  podManagementPolicy: Parallel # khởi động tất cả pod song song (tăng tốc)
  selector:
    matchLabels:
      app: kafka
  template:
    metadata:
      labels:
        app: kafka
    spec:
      terminationGracePeriodSeconds: 30

      # ── Affinity ──────────────────────────────────────────────────────────
      affinity:
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
              - matchExpressions:
                  - key: role
                    operator: In
                    values:
                      - kafka
        # Không cho 2 kafka pod cùng 1 node
        podAntiAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            - labelSelector:
                matchExpressions:
                  - key: app
                    operator: In
                    values:
                      - kafka
              topologyKey: "kubernetes.io/hostname"

      # ── Init container: format KRaft storage ──────────────────────────────
      initContainers:
        - name: kafka-init
          image: apache/kafka:4.0.0
          command:
            - /bin/bash
            - -c
            - |
              ORDINAL="${HOSTNAME##*-}"
              export KAFKA_NODE_ID=$(( ORDINAL * 10 + 10 ))
              
              if [ ! -f /kafka/data/meta.properties ]; then
                echo "Formatting KRaft storage for node $KAFKA_NODE_ID..."
                cp /tmp/kraft-init.properties /tmp/kraft-init-dynamic.properties
                sed -i "s/node.id=0/node.id=${KAFKA_NODE_ID}/g" /tmp/kraft-init-dynamic.properties
                
                /opt/kafka/bin/kafka-storage.sh format \
                  --config /tmp/kraft-init-dynamic.properties \
                  --cluster-id ${CLUSTER_ID} \
                  --ignore-formatted
              else
                echo "Storage already formatted, skipping."
              fi
          env:
            - name: CLUSTER_ID
              valueFrom:
                secretKeyRef:
                  name: kafka-cluster-id
                  key: CLUSTER_ID
          volumeMounts:
            - name: data
              mountPath: /kafka/data
            - name: kafka-init-config
              mountPath: /tmp/kraft-init.properties
              subPath: kraft-init.properties

      containers:
        - name: kafka
          image: apache/kafka:4.0.0
          # Dùng script wrapper để cấu hình KAFKA_NODE_ID thay vì dùng env cứng
          command:
            - /bin/bash
            - -c
            - |
              ORDINAL="${HOSTNAME##*-}"
              export KAFKA_NODE_ID=$(( ORDINAL * 10 + 10 ))
              exec /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/kraft/server.properties
          # ── Broker identity & Listeners ──────────────────────────────────────
          env:
            - name: POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: CLUSTER_ID
              valueFrom:
                secretKeyRef:
                  name: kafka-cluster-id
                  key: CLUSTER_ID
            - name: NODE_IP
              valueFrom:
                fieldRef:
                  fieldPath: status.hostIP
            # Port INSIDE: 9092, OUTSIDE: 9094 (để tránh conflict port)
            - name: KAFKA_ADVERTISED_LISTENERS
              value: "INSIDE://$(POD_NAME).kafka.kafka.svc.cluster.local:9092,OUTSIDE://$(NODE_IP):30094"
            - name: KAFKA_LISTENERS
              value: "INSIDE://:9092,OUTSIDE://:9094,CONTROLLER://:9093"

            # ── JMX ────────────────────────────────────────────────────────
            - name: JMX_PORT
              value: "1099"
            - name: KAFKA_JMX_OPTS
              value: >-
                -Dcom.sun.management.jmxremote
                -Dcom.sun.management.jmxremote.authenticate=false
                -Dcom.sun.management.jmxremote.ssl=false
                -Djava.rmi.server.hostname=127.0.0.1
                -Dcom.sun.management.jmxremote.rmi.port=$(JMX_PORT)
          envFrom:
            - configMapRef:
                name: kafka-configmap
          ports:
            - name: broker-inside
              containerPort: 9092
            - name: broker-outside
              containerPort: 9094
            - name: controller
              containerPort: 9093
            - name: jmx
              containerPort: 1099

          # ── Resources ────────────────────────────────────────────────────
          resources:
            requests:
              memory: "4000Mi"
              cpu: "1000m"
            limits:
              memory: "15000Mi"
              cpu: "2000m"

          # ── Liveness & Readiness ──────────────────────────────────────────
          livenessProbe:
            tcpSocket:
              port: 9092
            initialDelaySeconds: 60
            periodSeconds: 20
            failureThreshold: 3
          readinessProbe:
            tcpSocket:
              port: 9092
            initialDelaySeconds: 30
            periodSeconds: 10
            failureThreshold: 3
          volumeMounts:
            - name: data
              mountPath: /kafka/data
            - name: kafka-server-config
              mountPath: /opt/kafka/config/kraft/server.properties
              subPath: server.properties

      volumes:
        - name: kafka-init-config
          configMap:
            name: kafka-kraft-init-config
        - name: kafka-server-config
          configMap:
            name: kafka-kraft-server-config
      restartPolicy: Always

  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 101Gi
```

---

## **6. PodDisruptionBudget (PDB)**

Đảm bảo khi drain node hoặc rolling update, ít nhất **2 broker** vẫn còn sống (đủ ISR cho `min.insync.replicas=2`).

```yaml
# kafka-pdb.yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: kafka-pdb
  namespace: kafka
spec:
  minAvailable: 2
  selector:
    matchLabels:
      app: kafka
```

---

## **7. HorizontalPodAutoscaler (HPA)**

> ⚠️ **Lưu ý**: StatefulSet Kafka **không nên tự động scale ngang** vì mỗi broker có state (partition data) riêng. HPA dưới đây chỉ dùng để tự động scale **consumer** hay các service stateless khác.
> 
> Nếu muốn scale Kafka broker, hãy scale **thủ công** và rebalance partition sau đó.

Nếu vẫn muốn dùng HPA cho Kafka (ví dụ: scale từ 3 lên 5 broker tạm thời):

```yaml
# kafka-hpa.yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: kafka-hpa
  namespace: kafka
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: StatefulSet
    name: kafka
  minReplicas: 3
  maxReplicas: 5
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
  behavior:
    scaleDown:
      stabilizationWindowSeconds: 300  # chờ 5 phút trước khi scale down
      policies:
        - type: Pods
          value: 1
          periodSeconds: 600  # chỉ xóa 1 pod mỗi 10 phút
    scaleUp:
      stabilizationWindowSeconds: 60
      policies:
        - type: Pods
          value: 1
          periodSeconds: 120
```

---

## **8. Thứ tự apply**

```bash
#!/bin/bash

echo "🚀 Bắt đầu triển khai cụm Kafka KRaft..."

# 1. Namespace
echo "📦 1. Tạo Namespace..."
kubectl apply -f namespace.yaml

# 2. Secret (CLUSTER_ID – sinh trước, chỉ 1 lần)
echo "🔑 2. Khởi tạo Secret (CLUSTER_ID)..."
kubectl apply -f kafka-cluster-id-secret.yaml

# 3. ConfigMaps
echo "📜 3. Apply các ConfigMaps..."
kubectl apply -f kafka-configmap.yaml
kubectl apply -f kafka-kraft-server-config.yaml
kubectl apply -f kafka-kraft-init-config.yaml

# 4. Services
# Lưu ý: Phải tạo trước StatefulSet để nội bộ DNS resolve được
echo "🔌 4. Apply Services (Headless & External)..."
kubectl apply -f kafka-service-headless.yaml
kubectl apply -f kafka-service-external.yaml

# 5. StatefulSet
echo "🏢 5. Khởi chạy Kafka StatefulSet..."
kubectl apply -f kafka-statefulset.yaml

# 6. PodDisruptionBudget (PDB)
echo "🛡️ 6. Thiết lập PodDisruptionBudget..."
kubectl apply -f kafka-pdb.yaml

# 7. HorizontalPodAutoscaler (HPA - optional)
echo "📈 7. Thiết lập HPA..."
kubectl apply -f kafka-hpa.yaml

# Kiểm tra trạng thái Rollout
echo "⏳ Đang chờ Kafka khởi động (có thể mất vài phút)..."
kubectl rollout status statefulset/kafka -n kafka

echo "✅ Hoàn tất! Cụm Kafka của bạn đã sẵn sàng hoạt động."
```

---

## **9. Kiểm tra hệ thống**

### **9.1 Kiểm tra pods**

```bash
kubectl get pods -n kafka -o wide
# Mỗi pod phải chạy trên 1 node khác nhau (podAntiAffinity)
```

### **9.2 Kiểm tra KRaft metadata quorum**

```bash
kubectl exec -n kafka kafka-0 -- \
  /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server kafka-0.kafka.kafka.svc.cluster.local:9092 \
  describe --status
```

### **9.3 Kiểm tra broker list**

```bash
kubectl exec -n kafka kafka-0 -- \
  /opt/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server kafka-0.kafka.kafka.svc.cluster.local:9092
```

### **9.4 Tạo test topic**

```bash
kubectl exec -n kafka kafka-0 -- \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-0.kafka.kafka.svc.cluster.local:9092 \
  --create --topic test-topic \
  --partitions 3 \
  --replication-factor 3
```

### **9.5 Producer / Consumer test**

```bash
# Producer
kubectl exec -n kafka kafka-0 -it -- \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka-0.kafka.kafka.svc.cluster.local:9092 \
  --topic test-topic

# Consumer (terminal khác)
kubectl exec -n kafka kafka-1 -it -- \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-0.kafka.kafka.svc.cluster.local:9092 \
  --topic test-topic \
  --from-beginning
```

### **9.6 Kiểm tra JMX**

```bash
kubectl exec -n kafka kafka-0 -- \
  bash -c "echo '' | nc -w 1 localhost 1099 && echo JMX OK"
```

### **9.7 Kiểm tra PDB**

```bash
kubectl get pdb -n kafka
# DISRUPTIONS ALLOWED phải là 1 (3 replicas - minAvailable 2)
```

---

## **10. Lưu ý vận hành**

### **Rolling Update**

```bash
# Kafka StatefulSet update theo thứ tự ngược (kafka-2 trước)
kubectl set image statefulset/kafka kafka=apache/kafka:4.0.1 -n kafka
kubectl rollout status statefulset/kafka -n kafka
```

### **Scale thủ công (thêm broker)**

```bash
# Scale lên 5 broker
kubectl scale statefulset kafka --replicas=5 -n kafka

# Sau khi pod ready, rebalance partition
kubectl exec -n kafka kafka-0 -- \
  /opt/kafka/bin/kafka-reassign-partitions.sh \
  --bootstrap-server kafka-0.kafka.kafka.svc.cluster.local:9092 \
  --reassignment-json-file /tmp/reassign.json \
  --execute
```

### **Backup / Snapshot**

- PVC `/kafka/data` chứa toàn bộ log + KRaft metadata.
- Dùng Velero hoặc CSI snapshot để backup PVC định kỳ.
- Không xóa PVC khi xóa StatefulSet nếu muốn giữ data.