# Confluent Platform — Schema Registry テクニカルガイド

## 目次

1. [Schema Registry とは](#1-schema-registry-とは)
2. [基本概念](#2-基本概念)
3. [対応するスキーマ形式](#3-対応するスキーマ形式)
4. [動作のしくみとメッセージ形式](#4-動作のしくみとメッセージ形式)
5. [Subject と命名戦略](#5-subject-と命名戦略)
6. [互換性とスキーマの進化](#6-互換性とスキーマの進化)
7. [REST API 早見表](#7-rest-api-早見表)
8. [Confluent エコシステムとの連携](#8-confluent-エコシステムとの連携)
9. [クライアントと SerDes の設定](#9-クライアントと-serdes-の設定)
10. [ベストプラクティスとよくある落とし穴](#10-ベストプラクティスとよくある落とし穴)
11. [参考資料](#11-参考資料)

---

## 1. Schema Registry とは

- **集中管理型のスキーマリポジトリ**で、Kafka topic メッセージのスキーマを管理・検証し、直列化・逆直列化をサポートします。
- プロデューサー／コンシューマーはスキーマを通じてデータの一貫性と互換性を保ち、スキーマを安全に進化させられます。
- 2 つの要素から成ります：**REST サービス**（スキーマの検証・保存・取得）と、Kafka クライアントに組み込む**シリアライザー／デシリアライザー（SerDes）**。
- Confluent **Stream Governance** の中核コンポーネントで、Confluent Cloud（マネージド）と Confluent Platform（セルフホスト）の両方で利用できます。

### 1.1 解決する主な課題

| 課題 | Schema Registry の役割 |
|------|------------------------|
| データの不整合 | スキーマ契約を一元化し、汚いデータのリスクを低減 |
| 形式の非互換 | 進化時の互換性を強制的に検証 |
| ネットワーク負荷の増大 | メッセージには schema ID（4 バイト）だけを載せ、完全なスキーマは送らない |
| ガバナンス／監査の不足 | バージョン管理・リネージュ・監査の機能 |

---

## 2. 基本概念

| 概念 | 説明 |
|------|------|
| **Schema** | メッセージ構造の定義：フィールド、型、関係、制約 |
| **Subject** | Registry 内でのスキーマの命名単位。通常、1 つの topic の key / value がそれぞれ 1 つの subject に対応 |
| **Schema ID** | 各スキーマに割り当てられるグローバルに一意な整数 ID。オンラインで識別に使う |
| **Version** | 同一 subject 内でのスキーマのバージョン番号（1 から増加）。登録ごとに +1 |
| **Compatibility** | 新バージョンと旧バージョンの互換性を検証するルール |

- 1 つのスキーマは**グローバル**に一意な ID を持ち、**subject 内**では独立した増分バージョン番号を持ちます。
- 初回登録時に ID と version=1 を同時に割り当て。互換な新スキーマを再登録すると version が +1 され、ID は新しく割り当てられます。

---

## 3. 対応するスキーマ形式

| 形式 | 特徴 | 主な用途 |
|------|------|----------|
| **Avro** | バイナリ・コンパクト、スキーマ内蔵、進化に対応、動的型 | 最も一般的。Kafka エコシステムの事実上の標準 |
| **Protobuf** | 言語横断、`proto3`、gRPC エコシステムと相性が良い | 既存の Protobuf/gRPC 環境 |
| **JSON Schema** | テキスト・人間が読める、検証ルールに対応 | JSON の可読性／検証が必要な場面 |

> Schema Registry は同じ API で 3 形式を統一的に管理します。互換性ルールは形式ごとに定義されます（第 6 節参照）。

---

## 4. 動作のしくみとメッセージ形式

### 4.1 直列化の流れ

```
Producer                            Schema Registry                        Kafka
   │ 1. データを直列化し、スキーマを照会/登録 ──► schema ID を返す
   │ 2. [magic byte + schema ID + データ] でパッキング ───────────────────────────► topic に書き込み
Consumer ◄──────────────────────── 3. schema ID を読み、Registry からスキーマを取得して逆直列化
```

### 4.2 Confluent Wire Format（オンラインのバイナリ形式）

```
┌──────────┬────────────────┬─────────────┐
│ Magic(1B)│ Schema ID(4B)  │ データ本体    │
│ 0x00     │ ビッグエンディアン 4 バイト │ Avro/PB/... │
└──────────┴────────────────┴─────────────┘
```

- 先頭バイトの magic byte は固定の `0x00` で、「Schema Registry 対応」と通常のペイロードを区別します。
- 続く 4 バイトが schema ID（ビッグエンディアン）。
- コンシューマーはこの ID をもとに Registry から対応スキーマを取得して逆直列化します。

### 4.3 Schema ID Validation（ブローカー側の検証）

Confluent Server はブローカー側で、メッセージが持つ schema ID が Registry に登録済みかを検証し、不正なメッセージを書き込み前に遮断できます（未登録・削除済みスキーマのメッセージが topic に入るのを防ぎます）。

---

## 5. Subject と命名戦略

Subject は Registry 内でのスキーマの論理的な名前空間で、命名は**戦略**で決まります：

| 戦略 | subject 名 | 適用場面 |
|------|--------------|------|
| **TopicNameStrategy**（デフォルト） | `{topic}-key` / `{topic}-value` | 1 つの topic で 1 種類のスキーマを使う |
| **RecordNameStrategy** | レコードの完全修飾名（`namespace.name`） | 1 つの topic で複数のレコード型を扱う |
| **TopicRecordNameStrategy** | `{topic}-{recordName}` | topic を区別しつつ複数のレコード型を扱う |

```properties
# プロデューサー／コンシューマー側で命名戦略を指定（Avro の例）
key.subject.name.strategy=io.confluent.kafka.serializers.subject.TopicRecordNameStrategy
value.subject.name.strategy=io.confluent.kafka.serializers.subject.TopicRecordNameStrategy
```

---

## 6. 互換性とスキーマの進化

### 6.1 互換性の種類

| 種類 | 意味 |
|------|------|
| **BACKWARD**（デフォルト） | 新しいスキーマのコンシューマーが、古いスキーマで書かれたデータを読める |
| **BACKWARD_TRANSITIVE** | 新しいスキーマが**すべての過去バージョン**と後方互換 |
| **FORWARD** | 古いスキーマのコンシューマーが、新しいスキーマで書かれたデータを読める |
| **FORWARD_TRANSITIVE** | すべての過去バージョンと前方互換 |
| **FULL** | backward + forward を同時に満たす |
| **FULL_TRANSITIVE** | すべての過去バージョンと full 互換 |
| **NONE** | 互換性を検証しない |

### 6.2 Avro / Protobuf の互換ルール（抜粋）

| 変更 | BACKWARD | FORWARD | FULL |
|------|:---:|:---:|:---:|
| 省略可能フィールドの追加 | ✔ | ✔ | ✔ |
| 省略可能フィールドの削除 | ✔ | ✔ | ✔ |
| 必須フィールドの追加 | ✘ | ✔ | ✘ |
| 必須フィールドの削除 | ✔ | ✘ | ✘ |
| union/oneof バリアントの追加 | ✔ | ✘ | ✘ |
| union/oneof バリアントの削除 | ✘ | ✔ | ✘ |
| スカラー型の拡大（例 int→long） | ✔ | ✘ | ✘ |
| スカラー型の縮小 | ✘ | ✔ | ✘ |

### 6.3 JSON Schema の互換性ポリシー

JSON Schema には **互換性ポリシー**（`compatibilityPolicy`）と**コンテンツモデル**（`additionalProperties`）が 1 段階加わります：

- **ポリシー**：`STRICT`（デフォルト。コンテンツモデルに従い厳密に検証）または `LENIENT`（緩く、ほぼ許可）。
- **コンテンツモデル**：`additionalProperties: true`（オープン）と `false`（クローズド）。オープンな方が多くの変更を許容します。

```bash
# グローバルを LENIENT（緩く）に設定
curl -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"compatibilityPolicy": "LENIENT"}' http://localhost:8081/config

# 特定の subject だけ BACKWARD に設定
curl -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"compatibility": "BACKWARD"}' http://localhost:8081/config/{subject}
```

### 6.4 互換性設定の階層

優先順位は高い順に：**subject レベル > グローバル**。グローバル（`/config`）と subject レベル（`/config/{subject}`）で個別に設定でき、グローバルのデフォルトは `BACKWARD` です。

---

## 7. REST API 早見表

デフォルトポートは `8081`。よく使うエンドポイント：

| メソッド | パス | 説明 |
|------|------|------|
| GET | `/subjects` | すべての subject を一覧 |
| POST | `/subjects/{subject}/versions` | 新しいバージョンを登録（id を返す） |
| GET | `/subjects/{subject}/versions` | ある subject の全バージョンを一覧 |
| GET | `/subjects/{subject}/versions/{v}` | 指定バージョンを取得 |
| GET | `/schemas/ids/{id}` | グローバル ID でスキーマを取得 |
| POST | `/compatibility/subjects/{subject}/versions/{v}` | 互換性を検証 |
| DELETE | `/subjects/{subject}` | subject を削除 |
| PUT | `/config` / `/config/{subject}` | 互換性を設定 |
| GET/PUT | `/mode` | モードの読み書き（READWRITE / READONLY / IMPORT） |

```bash
# Avro スキーマを subject "orders-value" に登録
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema": "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}"}' \
  http://localhost:8081/subjects/orders-value/versions
```

---

## 8. Confluent エコシステムとの連携

| コンポーネント | 連携方法 |
|------|----------|
| **Kafka** | Schema ID Validation（ブローカー側の検証） |
| **Kafka Connect** | converter（`AvroConverter` / `ProtobufConverter` / `JsonSchemaConverter`）でスキーマ付きメッセージを読み書き |
| **ksqlDB / Flink** | `avro` / `protobuf` / `jsonschema` の直列化形式で読み書き |
| **REST Proxy** | `avro` / `protobuf` / `jsonschema` の embedded format でスキーマ/ID を直接渡す |
| **Control Center / Cloud Console** | スキーマの閲覧・登録・検証を行う UI |
| **CLI / クライアント** | SerDes が自動で登録・取得 |

---

## 9. クライアントと SerDes の設定

```java
// Producer の例（Avro）
props.put("bootstrap.servers", "localhost:9092");
props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
props.put("schema.registry.url", "http://localhost:8081");
```

| 設定 | 説明 |
|------|------|
| `schema.registry.url` | Registry のアドレス（複数可、カンマ区切り） |
| `auto.register.schemas` | 自動登録するか（`KafkaAvroSerializer` はデフォルト true） |
| `use.latest.version` | 逆直列化時に書き込み時のバージョンではなく最新版を使うか（デフォルト false） |
| `key/value.subject.name.strategy` | 命名戦略 |
| `value.schema.id` / `value.subject` | ID / subject を手動指定（特殊な用途） |

---

## 10. ベストプラクティスとよくある落とし穴

**ベストプラクティス**

1. **デフォルトの BACKWARD で多くの場面は十分**：省略可能フィールド（デフォルト値付き）を追加するのを優先し、後方互換を保つ。
2. **フィールド設計でデフォルト値を意識**：Avro でフィールドを追加するときはデフォルト値を付ける。削除するときは元々省略可能／デフォルト値ありにする。
3. **複数型の topic は record 命名戦略を使う**：1 つの topic に複数イベントを混ぜるなら `RecordNameStrategy` か `TopicRecordNameStrategy`。
4. **移行時は READONLY か IMPORT モードに**：アップグレード／移行中に誤登録を防ぐ。
5. **Protobuf の参照で共通スキーマを再利用**：共通型の重複定義を避ける。

**よくある落とし穴**

- **スキーマの誤削除**：オンラインのメッセージが削除済みスキーマを参照すると逆直列化に失敗。削除は慎重に。
- **namespace/name の衝突**：Avro の record が同名・同 namespace なのにフィールドが異なると、登録失敗または再利用される。
- **JSON Schema のクローズドモデルでフィールド追加**：`additionalProperties: false` のときにフィールドを追加すると互換性が壊れる。
- **compatibility を NONE にする**：進化の保護がなくなり、壊れたデータを本番に流しやすい。
- **topic と subject の名前が異なる**：subject を手動で作る場合や非デフォルトの命名戦略を使う場合、key/value の接尾辞 `-key`/`-value` に注意。

---

## 11. 参考資料

- [Schema Registry 公式ドキュメント](https://docs.confluent.io/platform/current/schema-registry/index.html)
- [スキーマの進化と互換性](https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html)
- [Schema Registry API Reference](https://docs.confluent.io/platform/current/schema-registry/develop/api.html)
- [Formats, Serializers, and Deserializers](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html)

---

> 注記：本ドキュメントは Confluent Platform 8.3（Kafka 4.3）の公式ドキュメントに基づいて整理したものです。互換性ルールや JSON Schema ポリシーなどの詳細はバージョンによって進化するため、実際の使用時はご利用のバージョンに対応する公式ドキュメントを参照してください。
