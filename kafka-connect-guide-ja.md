# Confluent Platform — Kafka Connect テクニカルガイド

## 目次

1. [Kafka Connect とは](#1-kafka-connect-とは)
2. [基本概念](#2-基本概念)
3. [アーキテクチャと実行モード](#3-アーキテクチャと実行モード)
4. [Source と Sink コネクター](#4-source-と-sink-コネクター)
5. [設定の早見表](#5-設定の早見表)
6. [Converter（コンバーター）](#6-converterコンバーター)
7. [SMT（単一メッセージ変換）](#7-smt単一メッセージ変換)
8. [Dead Letter Queue（デッドレターキュー）](#8-dead-letter-queueデッドレターキュー)
9. [REST API 早見表](#9-rest-api-早見表)
10. [ベストプラクティスとよくある落とし穴](#10-ベストプラクティスとよくある落とし穴)
11. [参考資料](#11-参考資料)

---

## 1. Kafka Connect とは

- **無償・オープンソースの Kafka コンポーネント**で、集中型のデータハブとして、データベース、キーバリューストア、検索インデックス、ファイルシステムと Kafka の間でデータを統合します。
- **Source コネクター**：外部システムのデータを Kafka topic へ取り込む（全量 CDC やメトリクス収集など）。
- **Sink コネクター**：Kafka topic のデータを外部システムへ書き出す（ES、Hadoop、データウェアハウスなど）。
- Kafka + ストリーム処理フレームワークと組み合わせて、ETL パイプラインの重要な一環を担います。

### 1.1 主な価値

| 特徴 | 説明 |
|------|------|
| データ中心 | 意味のある抽象（record/schema）でデータを取得・送信 |
| 柔軟で拡張可能 | 単一マシン（standalone）から組織レベルのサービス（distributed）まで |
| 再利用・拡張可能 | 既存のコネクターや自作 plugin を使い、導入までの時間を短縮 |
| 耐障害性 | offset を自動管理し、失敗時はリトライ可能 |

---

## 2. 基本概念

| 概念 | 説明 |
|------|------|
| **Connector（コネクター）** | 高レベルの抽象。データフローを調整し、「データがどこから来てどこへ行くか」を定義 |
| **Task（タスク）** | 実際にデータを運ぶ実行単位。1 つの connector が複数の task に分かれて並列実行 |
| **Worker（ワーカー）** | connector と task を実行するプロセス（standalone または distributed） |
| **Converter（コンバーター）** | Connect のレコードとオンラインのバイト列の間の直列化（Avro/JSON/Protobuf/...） |
| **Transform / SMT** | 単一メッセージ変換。通過する各レコードに軽量な書き換えを行う |
| **Dead Letter Queue（DLQ）** | 処理に失敗したレコードを書き込むデッドレター topic |

> **connector plugin**（インストールされたクラス実装）と **connector instance**（実行中のインスタンス）を区別してください。「コネクターをインストールする」は plugin、「コネクターの状態を確認する」は instance を指します。

---

## 3. アーキテクチャと実行モード

### 3.1 Standalone モード

- 単一プロセスがすべての connector と task を実行。設定はローカルの properties ファイル。
- 開発／テストや単一マシンのログ収集向け。耐障害性・自動再均衡はなし。

### 3.2 Distributed モード

- 複数の worker がクラスタを構成し、connector と task を **leader worker** が割り当て。自動再均衡・耐障害性あり。
- 設定は REST API で投入し、Kafka の内部 topic に保存。worker はステートレス。

### 3.3 内部ストレージ topic（分散モード）

| 設定 | デフォルト topic | 用途 |
|------|-----------|------|
| `config.storage.topic` | `connect-configs` | connector 設定の保存 |
| `offset.storage.topic` | `connect-offsets` | 各 task の offset の保存 |
| `status.storage.topic` | `connect-status` | task 状態の保存 |

### 3.4 offset 管理

- Source connector は「ソースシステムのどこまで読んだか」、Sink connector は「Kafka のどの offset まで消費したか」を記録します。
- 分散モードでは `connect-offsets` topic に永続化し、再起動後も前回位置から続行します。

---

## 4. Source と Sink コネクター

| タイプ | 方向 | 例 |
|------|------|------|
| **Source** | 外部システム → Kafka | Debezium MySQL/Postgres CDC、JDBC、S3、Salesforce |
| **Sink** | Kafka → 外部システム | S3、Elasticsearch、JDBC、BigQuery、Snowflake、MongoDB |

- Confluent は多数の**組み込みコネクター**（JDBC、S3、Elasticsearch、Debezium CDC シリーズなど）を提供し、**Confluent Hub**（`confluent-hub install`）からインストールできます。
- Confluent Cloud は**フルマネージドコネクター**（GUI 設定、柔軟なスケーリング、運用不要）を提供します。

---

## 5. 設定の早見表

コネクター設定の例（分散モード、REST API で投入）：

```json
{
  "name": "jdbc-source-orders",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "tasks.max": "4",
    "connection.url": "jdbc:mysql://db:3306/orders",
    "connection.user": "etl",
    "connection.password": "***",
    "mode": "timestamp+incrementing",
    "timestamp.column.name": "updated_at",
    "incrementing.column.name": "id",
    "topic.prefix": "mysql-orders-",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://localhost:8081",
    "value.converter.schema.registry.url": "http://localhost:8081"
  }
}
```

| 設定 | 説明 |
|------|------|
| `name` | コネクターインスタンス名（一意） |
| `connector.class` | コネクター実装クラス |
| `tasks.max` | 最大並列 task 数 |
| `key.converter` / `value.converter` | key/value コンバーター |
| `transforms` | SMT エイリアスの一覧（カンマ区切り） |
| `errors.tolerance` | `none`（デフォルト。失敗で停止）/ `all`（エラーを飲み込み継続） |
| `errors.deadletterqueue.topic.name` | DLQ を有効にする topic |

---

## 6. Converter（コンバーター）

| Converter | 説明 |
|-----------|------|
| `AvroConverter` | Avro を読み書き。Schema Registry と連携 |
| `ProtobufConverter` | Protobuf を読み書き。Schema Registry と連携 |
| `JsonSchemaConverter` | JSON Schema を読み書き。Schema Registry と連携 |
| `JsonConverter` | 純粋な JSON（スキーマなし） |
| `StringConverter` / `ByteArrayConverter` | 文字列 / バイト列 |

```properties
# Avro + Schema Registry を使う
value.converter=io.confluent.connect.avro.AvroConverter
value.converter.schema.registry.url=http://localhost:8081
value.converter.value.subject.name.strategy=io.confluent.kafka.serializers.subject.TopicNameStrategy
```

---

## 7. SMT（単一メッセージ変換）

SMT は source がデータを出力した後 Kafka へ書き込む前（または sink が取得した後、外部へ書き込む前）に、**単一のレコード**を書き換えます。別途ストリーム処理プログラムを用意せずに済みます。

よく使う SMT：

| SMT | 役割 |
|-----|------|
| `Cast` | フィールド／key・value 全体の型変換 |
| `ExtractField` | 単一フィールドを value として抽出 |
| `InsertField` | メタデータや静的フィールドを挿入 |
| `MaskField` | フィールドをマスク（null に置換） |
| `ValueToKey` | value のサブセットを key にする |
| `ReplaceField` | フィールドのフィルタ／リネーム |
| `Flatten` | ネスト構造のフラット化 |
| `SetSchemaMetadata` | スキーマ名／バージョンを設定 |
| `TimestampConverter` | タイムスタンプ形式の相互変換 |
| `RegexRouter` / `TimestampRouter` | 正規表現／時刻で出力先 topic を書き換え |
| `DropHeaders` / `InsertHeader` | header の削除／挿入 |

```json
{
  "transforms": "mask, route",
  "transforms.mask.type": "org.apache.kafka.connect.transforms.MaskField$Value",
  "transforms.mask.fields": "card_number, cvv",
  "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
  "transforms.route.regex": ".*",
  "transforms.route.replacement": "orders-$0"
}
```

---

## 8. Dead Letter Queue（デッドレターキュー）

変換／処理に失敗したレコードを独立した topic に書き込み、パイプライン全体の停止を防ぎ、後からの調査・再実行を可能にします。

```properties
errors.tolerance=all
errors.deadletterqueue.topic.name=dlq-orders
errors.deadletterqueue.topic.replication.factor=3
errors.deadletterqueue.context.headers.enable=true
```

| 設定 | 説明 |
|------|------|
| `errors.tolerance` | `none`（デフォルト。エラーで停止）/ `all`（エラーを飲み込み継続） |
| `errors.deadletterqueue.topic.name` | デッドレター topic |
| `errors.deadletterqueue.context.headers.enable` | header にエラーコンテキストを記録 |
| `errors.log.enable` | エラーをログに書くか |

---

## 9. REST API 早見表

分散モードのデフォルトポートは `8083`。

| メソッド | パス | 説明 |
|------|------|------|
| GET | `/connectors` | すべての connector インスタンスを一覧 |
| POST | `/connectors` | connector を作成 |
| GET | `/connectors/{name}` | 設定を確認 |
| GET | `/connectors/{name}/status` | 状態を確認 |
| POST | `/connectors/{name}/restart` | connector を再起動 |
| PUT | `/connectors/{name}/config` | 設定を更新 |
| DELETE | `/connectors/{name}` | 削除 |
| GET | `/connector-plugins` | インストール済み plugin を一覧 |

```bash
curl -s localhost:8083/connectors
curl -s localhost:8083/connectors/jdbc-source-orders/status
```

---

## 10. ベストプラクティスとよくある落とし穴

**ベストプラクティス**

1. **Avro + Schema Registry を使う**：スキーマ進化の余地を残し、生の JSON より堅牢。
2. **`tasks.max` を適切に**：ソースシステムのパーティション／主キー分布に制約される。大きすぎても無益。
3. **JDBC source は `timestamp+incrementing`**：新規行と更新行の両方を扱え、純粋な bulk よりリアルタイム。
4. **機密フィールドは `MaskField`**：Kafka へ書く前にマスク。
5. **DLQ を有効に**：本番では `errors.tolerance=all` + DLQ で、1 件の壊れたデータがタスク全体を止めるのを防ぐ。

**よくある落とし穴**

- **`schema.registry.url` の設定漏れ**：Avro converter を使うのに設定し忘れると、タスクが起動しない。
- **SMT の順序**：`transforms` はカンマ区切りで、**実行順序は記載順**。逆に書くと結果がおかしくなる。
- **`mode` と主キー**：JDBC の増分モードは増分列／タイムスタンプ列が必要。なければ全件スキャンになる。
- **offset の喪失**：`connect-offsets` topic を誤って消すと、重複やデータ喪失が起きる。
- **distributed モードの設定分散**：設定は Kafka topic に保存されるため、調査時は `/connectors/{name}` で実際の設定を確認する。

---

## 11. 参考資料

- [Kafka Connect 公式ドキュメント](https://docs.confluent.io/platform/current/connect/index.html)
- [SMT リファレンス](https://docs.confluent.io/platform/current/connect/transforms/overview.html)
- [Connect REST API](https://docs.confluent.io/platform/current/connect/references/restapi.html)
- [Confluent Hub（コネクターカタログ）](https://www.confluent.io/hub/)

---

> 注記：本ドキュメントは Confluent Platform 8.3（Kafka 4.3）の公式ドキュメントに基づいて整理したものです。コネクター／SMT の詳細はバージョンによって進化するため、実際の使用時はご利用のバージョンに対応する公式ドキュメントを参照してください。
