# Confluent Platform — REST Proxy と Control Center テクニカルガイド

## 目次

1. [REST Proxy とは](#1-rest-proxy-とは)
2. [API バージョンとコンテンツタイプ](#2-api-バージョンとコンテンツタイプ)
3. [よく使うエンドポイント早見表](#3-よく使うエンドポイント早見表)
4. [デプロイと設定](#4-デプロイと設定)
5. [Control Center とは](#5-control-center-とは)
6. [Control Center の主な機能](#6-control-center-の主な機能)
7. [ベストプラクティスとよくある落とし穴](#7-ベストプラクティスとよくある落とし穴)
8. [参考資料](#8-参考資料)

---

## 1. REST Proxy とは

- Kafka クラスタに **RESTful インターフェース**を提供し、ネイティブの Kafka プロトコルやクライアントなしでメッセージの生成・消費、クラスタ状態の確認、管理操作ができます。
- 主な用途：**公式クライアント非対応の言語**（やスクリプト）でのデータ送信、Kafka 未対応のストリーム処理フレームワークへのデータ供給、スクリプトによる管理操作。

### 1.1 2 つの形態

| 形態 | 説明 |
|------|------|
| **Confluent Server 内蔵** | Confluent Server（broker）をデプロイすると、完全な Admin REST API がそのまま公開される |
| **独立 REST Proxy** | 独立した `kafka-rest` ノードをデプロイし、Produce/Consume API を追加提供（API v3 から Admin API も提供） |

> 完全な Admin API を提供するのは **Confluent Server だけ**です。独立 REST Proxy はオープンソース Kafka の REST API サブセットのみを公開します（第 3 節参照）。

---

## 2. API バージョンとコンテンツタイプ

### 2.1 バージョン

| バージョン | 説明 |
|------|------|
| **v2** | 安定版で広く使われる Produce/Consume/Metadata API |
| **v3** | 新バージョン。v3 から Admin API を追加。Content-Type は `application/json` |

### 2.2 メディアタイプの形式

```
application/vnd.kafka[.embedded_format].[api_version]+[serialization_format]
```

- **embedded format**：`json`、`binary`、`avro`、`protobuf`、`jsonschema`（データ本体の実際の形式）。
- **serialization format**：外側のラッパーで、通常は `json`。
- **api version**：`v2` / `v3`。

| 場面 | Content-Type |
|------|--------------|
| JSON データ（v2） | `application/vnd.kafka.json.v2+json` |
| Avro（v2） | `application/vnd.kafka.avro.v2+json` |
| Protobuf（v2） | `application/vnd.kafka.protobuf.v2+json` |
| JSON Schema（v2） | `application/vnd.kafka.jsonschema.v2+json` |
| バイナリ（v2） | `application/vnd.kafka.binary.v2+json` |
| v3（統一） | `application/json` |

> `avro` / `protobuf` / `jsonschema` を使う場合、データは JSON + schema または schema ID として直接埋め込まれ、Schema Registry が直列化を行います（オンラインには schema ID のみ保存）。

---

## 3. よく使うエンドポイント早見表

### 3.1 生成（Produce）

```bash
# topic "orders" に JSON メッセージを 1 件生成（v2）
curl -X POST -H "Content-Type: application/vnd.kafka.json.v2+json" \
  --data '{"records":[{"value":{"id":1,"amount":100}}]}' \
  http://localhost:8082/topics/orders
```

| エンドポイント | 説明 |
|------|------|
| `POST /topics/{topic}` | 生成（key/value/partition を含められる） |
| `POST /topics/{topic}/partitions/{partition}` | 指定パーティションに生成 |

### 3.2 消費（Consume）

```bash
# 1) consumer インスタンスを作成
curl -X POST -H "Content-Type: application/vnd.kafka.v2+json" \
  --data '{"name":"my_consumer","format":"json","auto.offset.reset":"earliest"}' \
  http://localhost:8082/consumers/my_group

# 2) topic を購読
curl -X POST -H "Content-Type: application/vnd.kafka.v2+json" \
  --data '{"topics":["orders"]}' \
  http://localhost:8082/consumers/my_group/instances/my_consumer/subscription

# 3) レコードを取得
curl -X GET -H "Accept: application/vnd.kafka.json.v2+json" \
  http://localhost:8082/consumers/my_group/instances/my_consumer/records

# 4) インスタンスを閉じる
curl -X DELETE http://localhost:8082/consumers/my_group/instances/my_consumer
```

### 3.3 メタデータ（Metadata）

| エンドポイント | 説明 |
|------|------|
| `GET /topics` | topic を一覧 |
| `GET /topics/{topic}` | topic 詳細を確認 |
| `GET /topics/{topic}/partitions` | パーティションを一覧 |
| `GET /clusters/{cluster_id}` | クラスタ情報 |

### 3.4 Admin API（v3 / Confluent Server）

| エンドポイント | 説明 |
|------|------|
| `GET/POST /v3/clusters/{id}/topics` | topic を管理 |
| `GET /v3/clusters/{id}/configs/{name}` | 設定を照会 |
| `DELETE /v3/clusters/{id}/topics/{topic}` | topic を削除 |

> Confluent Server はさらに Configs、Broker、Cluster Linking、Balancer、Replica Status などの Admin エンドポイントを提供します。独立 REST Proxy はオープンソースのサブセットのみです。

---

## 4. デプロイと設定

```properties
# kafka-rest.properties（独立 REST Proxy）
bootstrap.servers=localhost:9092
listeners=http://0.0.0.0:8082
schema.registry.url=http://localhost:8081
```

- デフォルトの待受ポートは `8082`。
- 本番で Confluent Enterprise を使う場合は **REST Proxy Security Plugin** を導入し、リクエストを認証して主体を Kafka へ伝播します（マルチテナントのセキュリティ機能に対応）。
- グローバルレベルの設定（`compression.type` など）は properties で行い、リクエスト単位の設定には対応しません。

---

## 5. Control Center とは

- Confluent が提供する **Web ベースの管理・監視 UI** で、Kafka クラスタ、コネクター、schema、ストリーム処理などを可視化して運用します。
- Confluent Platform と一緒にデプロイします（セルフホスト）。Confluent Cloud には対応する Cloud Console があります。
- 運用者／開発者がコマンドラインなしで topic 管理、消費ラグの調査、コネクター管理、schema 閲覧などを行えます。

---

## 6. Control Center の主な機能

| 機能 | 説明 |
|------|------|
| **クラスタ監視** | broker/topic/partition のスループット、遅延、I/O、リソース使用状況 |
| **Topic 管理** | topic の作成・確認・設定、メッセージブラウザーによる閲覧 |
| **コンシューマー監視** | 消費グループの lag、消費レート、メンバー状態 |
| **Connect 管理** | コネクターとタスク状態の確認・追加・管理 |
| **Schema Registry 管理** | schema とバージョンの閲覧・登録・検証 |
| **ksqlDB エディター** | 組み込みのストリーミング SQL 照会 UI |
| **アラートとアクション** | メトリクスに基づくアラート、自動アクション（Trigger/Action） |
| **RBAC 連携** | きめ細かな権限と監査 |
| **ストリームリネージュ（Stream Lineage）** | エンドツーエンドのデータフローリネージュ追跡（データガバナンス） |
| **Replicator / Cluster Linking 監視** | クラスタ間レプリケーションの状態 |

---

## 7. ベストプラクティスとよくある落とし穴

**ベストプラクティス**

1. **REST Proxy は「非 Kafka クライアント」の場面に限定**：ネイティブクライアント（Java/C/librdkafka）の方が性能・セマンティクスに優れ、REST 層には追加のオーバーヘッドがある。
2. **生成と消費を分離**：consumer はステートフル（特定の REST Proxy インスタンスに紐づく）。高スループットには複数の consumer インスタンスが必要。
3. **Content-Type と Accept を明示**：最も具体的な media type（`application/vnd.kafka.json.v2+json`）を指定し、バージョンの揺れを避ける。
4. **本番では Security Plugin を有効に**：REST の認証主体を Kafka へ伝播し、RBAC と組み合わせる。
5. **Control Center は運用者に読み取り専用で**：書き込み操作（topic 削除など）は RBAC で権限を絞る。

**よくある落とし穴**

- **Content-Type の誤り**：`.v2+json` を忘れたり embedded format が一致しなかったりすると、406/415 が返る。
- **consumer を閉じない**：REST の consumer インスタンスは明示的に DELETE する必要があり、しないとリソースを占有する。
- **null レコード**：CP 8.0 から、key/value が null のときは対応する `key_schema`/`value_schema` フィールドを省略する必要がある。
- **SNI/Jetty の変更（CP 8.0+）**：REST クライアントは SNI header を付けるか、`confluent.http.server.sni.host.check.enabled=false` を設定する。
- **Control Center は代替ではない**：観測と管理のためのもので、データ移送（Connect が担う）やストリーム計算（Flink/ksqlDB が担う）は行わない。

---

## 8. 参考資料

- [REST Proxy 公式ドキュメント](https://docs.confluent.io/platform/current/kafka-rest/index.html)
- [REST Proxy API Reference](https://docs.confluent.io/platform/current/kafka-rest/api.html)
- [Control Center 公式ドキュメント](https://docs.confluent.io/platform/current/control-center/index.html)
- [Admin REST APIs](https://docs.confluent.io/platform/current/kafka-rest/admin-api.html)

---

> 注記：本ドキュメントは Confluent Platform 8.3（Kafka 4.3）の公式ドキュメントに基づいて整理したものです。API の詳細はバージョンによって進化します（例：CP 8.0 の Jetty 12 と null レコードの挙動変更）。実際の使用時はご利用のバージョンに対応する公式ドキュメントを参照してください。
