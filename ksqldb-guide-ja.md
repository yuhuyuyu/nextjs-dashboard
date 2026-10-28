# Confluent Platform — ksqlDB テクニカルガイド

---

## 目次

1. [ksqlDB とは](#1-ksqldb-とは)
2. [基本概念](#2-基本概念)
3. [アーキテクチャと動作のしくみ](#3-アーキテクチャと動作のしくみ)
4. [コンポーネント](#4-コンポーネント)
5. [デプロイとインストール](#5-デプロイとインストール)
6. [SQL 構文の早見表](#6-sql-構文の早見表)
7. [クエリの種類](#7-クエリの種類)
8. [結合（Join）と集約（Aggregation）](#8-結合joinと集約aggregation)
9. [コネクター（Connectors）](#9-コネクターconnectors)
10. [カスタム関数（UDF / UDAF / UDTF）](#10-カスタム関数udf--udaf--udtf)
11. [主なユースケース](#11-主なユースケース)
12. [ksqlDB と他のストリーム処理基盤の比較](#12-ksqldb-と他のストリーム処理基盤の比較)
13. [ベストプラクティスとよくある制約](#13-ベストプラクティスとよくある制約)
14. [参考資料](#14-参考資料)

---

## 1. ksqlDB とは

**ksqlDB** は Confluent が提供するストリーミング SQL データベースで、開発者が **SQL 構文**を使ってリアルタイムかつイベント駆動のストリーム処理アプリケーションを構築できるようにするものです。内部はすべて **Kafka Streams** の上に構築されており、SQL 文を Kafka Streams のトポロジーにコンパイルして実行します。

### 1.1 主な機能

- **SQL インターフェース**：SQL でリアルタイムのストリーミングデータを照会・変換・エンリッチ・結合でき、Java/Scala のコードは不要です。
- **永続クエリ（Persistent Query）**：`CREATE ... AS SELECT` は継続的に実行され、結果がリアルタイムに新しい Kafka トピックへ書き込まれます。
- **マテリアライズドビュー（Materialized View）**：集約結果がテーブルとして継続的に実体化され、照会できます。
- **プッシュ / プルクエリ**：継続的に更新される結果を購読する（Push）ことも、現在の状態を都度参照する（Pull）こともできます。
- **Exactly-Once セマンティクス**：エンドツーエンドの「正確に 1 回」の処理を保証します。
- **スケーラビリティと耐障害性**：Kafka のコンシューマーグループの仕組みで水平スケールし、状態は RocksDB に保存、changelog トピック経由で耐障害性を確保します。
- **ネイティブ統合**：Confluent Platform / Confluent Cloud と深く統合されており、Schema Registry、Kafka Connect コネクター、Control Center（UI と監視）、RBAC 権限管理が使えます。

### 1.2 ディストリビューション

| ディストリビューション | 説明 |
|--------|------|
| **ksqlDB for Confluent Platform** | セルフホスト型。Confluent Platform と一緒にデプロイし、RBAC などのエンタープライズ機能を含む |
| **ksqlDB for Confluent Cloud** | マネージドサービス。Confluent が運用 |

### 1.3 ⚠️ 重要な戦略上の注意

> **新しいストリーム処理のワークロードについては、現時点で Confluent は Confluent Platform for Apache Flink® を推奨しています。**
> ksqlDB は既存アプリケーションに対しては**引き続き完全にサポート（fully supported）**されますが、新規プロジェクトでは Flink の検討が推奨されます。本ドキュメントは ksqlDB そのものに焦点を当て、既存システムの理解と運用に役立てるためのものです。

---

## 2. 基本概念

### 2.1 ストリーム（Stream）とテーブル（Table）

| 観点 | STREAM（ストリーム） | TABLE（テーブル） |
|------|--------------|-------------|
| 意味 | 不変で追記型（append-only）のイベント列 | 変更可能で更新される「現在の状態」のスナップショット |
| 裏側の Kafka | イベントログ（event log） | 圧縮ログ（compacted log）または集約の changelog |
| たとえ | イベントの流れ（取引の記録ごと） | 状態テーブル（ユーザーの最新残高） |
| 更新の仕方 | 追記のみで削除しない | キー単位で上書き更新 |

- **ストリーム**は「何が起きたか」（facts）を表します。
- **テーブル**は「今どうなっているか」（current state）を表します。

### 2.2 クエリの種類（Query Types）

| 種類 | 説明 | 永続か |
|------|------|----------|
| **永続クエリ（Persistent Query）** | `CREATE STREAM/TABLE ... AS SELECT`。継続実行され、結果を新トピックへ書き込む | はい |
| **プッシュクエリ（Push Query）** | `SELECT ... EMIT CHANGES;`。結果の変化を継続的に配信する | いいえ（購読型） |
| **プルクエリ（Pull Query）** | `SELECT ... WHERE key = ...;`。キーで実体化テーブルの現在値を参照する | いいえ（即時返却） |

### 2.3 データモデル

- 各レコードは **key** と **value** で構成されます（Kafka のメッセージモデル）。
- **スキーマ**：Schema Registry で管理（Avro、Protobuf、JSON Schema に対応）。またはプリミティブな形式（JSON、CSV、DELIMITED、KAFKA）も使用できます。
- **シリアライズ形式**：`FORMAT` プロパティで指定します（例：`JSON`, `AVRO`, `PROTOBUF`, `JSON_SR`）。

---

## 3. アーキテクチャと動作のしくみ

### 3.1 実行モデル

ksqlDB のクエリ実行フロー：

```
SQL 文
   │
   ▼
ANTLR パーサー → AST（抽象構文木）
   │
   ▼
論理プラン（Logical Plan、演算子ツリー）
   │
   ▼
物理プラン（Physical Plan）→ Kafka Streams アプリケーションへコンパイル
   │
   ▼
永続クエリとしてクラスターにデプロイ
```

### 3.2 内部の主要メカニズム

- **コマンドトピック（Command Topic）**：ksqlDB クラスターのメタデータストア。すべての DDL/DML 文は内部のコマンドトピック（デフォルト `_confluent-ksql-<service_id>command_topic`）に書き込まれ、クラスター全体の整合性を保ちます。
- **Kafka Streams トポロジー**：各永続クエリは 1 つの Kafka Streams アプリケーションに対応し、入力トピックを消費して処理し、出力トピックへ書き出します。
- **状態ストア**：状態を持つ操作（集約、結合）の状態は **RocksDB**（ローカル）に保存され、changelog トピックでバックアップすることで障害復旧を実現します。
- **水平スケール**：Kafka のコンシューマーグループの仕組みを使い、ksqlDB サーバーのノードを増やすことで処理能力を拡張します。
- **Exactly-Once**：Kafka のトランザクションと冪等性の仕組みに依存して、エンドツーエンドで「正確に 1 回」を実現します。

### 3.3 アーキテクチャ図

```
┌─────────────────────────────────────────────┐
│                クライアント                  │
│  CLI / REST API / Control Center / 独自アプリ │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│          ksqlDB Server（複数ノード可）        │
│  · SQL の解析・コンパイル                     │
│  · Kafka Streams トポロジーの実行             │
│  · RocksDB の状態ストア                      │
└──────┬──────────────────┬───────────────────┘
       │ 読み書き          │ メタデータ
       ▼                  ▼
┌─────────────┐   ┌──────────────────┐   ┌───────────────────┐
│  Kafka       │   │ Command Topic    │   │ Schema Registry   │
│  (入力/出力) │   │ (クラスターメタデータ) │   │ (スキーマ管理)     │
└─────────────┘   └──────────────────┘   └───────────────────┘
```

---

## 4. コンポーネント

| コンポーネント | 説明 |
|------|------|
| **ksqlDB Server** | コアエンジン。SQL を解析・実行し、ストリーム処理トポロジーを動かし、REST API を公開する |
| **ksqlDB CLI** | 対話型コマンドラインツール（`ksql`）。サーバーに接続して文を送る |
| **REST API** | HTTP インターフェース（`/ksql`, `/query`, `/info`, `/healthcheck` など）。プログラムからの利用向け |
| **Control Center** | Confluent の Web UI。ksqlDB クエリを可視的に記述・監視できる |
| **UDF/UDAF/UDTF** | カスタムのスカラー関数 / 集約関数 / テーブル関数 |

---

## 5. デプロイとインストール

### 5.1 単体起動（開発・テスト）

```bash
# ksqlDB サーバーを起動（事前に Kafka ブローカーが必要）
bin/ksql-server-start config/ksql-server.properties

# CLI で接続
bin/ksql http://localhost:8088
```

### 5.2 Confluent Platform（本番）

- `confluent` の配布パッケージに含まれ、systemd / Docker / Kubernetes でデプロイします。
- 主要な設定ファイル `ksql-server.properties` のよく使うパラメーター：

| パラメーター | 説明 |
|------|------|
| `ksql.service.id` | ksqlDB クラスターの一意な ID（同じ service id のノードが 1 つのクラスターを構成する） |
| `bootstrap.servers` | Kafka ブローカーのアドレス |
| `ksql.schema.registry.url` | Schema Registry のアドレス |
| `listeners` | REST API のリッスンアドレス |
| `ksql.streams.num.standby.replicas` | 状態のスタンバイレプリカ数（高可用性向け） |

### 5.3 よく使う CLI コマンド

```bash
SHOW STREAMS;           -- すべてのストリームを表示
SHOW TABLES;            -- すべてのテーブルを表示
SHOW QUERIES;           -- 実行中のクエリを表示
SHOW TOPICS;            -- トピックを表示
DESCRIBE <name>;        -- 構造を表示
EXPLAIN <query>;        -- 実行プランを表示
TERMINATE <query_id>;   -- クエリを停止
DROP STREAM/TABLE <name>; -- 削除
```

---

## 6. SQL 構文の早見表

### 6.1 ストリームの作成

```sql
CREATE STREAM pageviews (
    viewtime BIGINT,
    userid VARCHAR,
    pageid VARCHAR
) WITH (
    KAFKA_TOPIC = 'pageviews',
    VALUE_FORMAT = 'JSON',
    PARTITIONS = 3
);
```

### 6.2 テーブルの作成

```sql
CREATE TABLE users (
    userid VARCHAR PRIMARY KEY,
    username VARCHAR
) WITH (
    KAFKA_TOPIC = 'users',
    VALUE_FORMAT = 'AVRO',
    KEY_FORMAT = 'KAFKA'
);
```

> テーブルには `PRIMARY KEY` が必要で、Kafka メッセージの key に対応します。

### 6.3 永続的な変換 — CSAS / CTAS

```sql
-- CREATE STREAM AS SELECT：ストリームからストリームへ
CREATE STREAM enriched_views AS
    SELECT p.viewtime, p.userid, u.username
    FROM pageviews p
    LEFT JOIN users u ON p.userid = u.userid
    EMIT CHANGES;

-- CREATE TABLE AS SELECT：集約して実体化テーブルへ
CREATE TABLE pageviews_per_user AS
    SELECT userid, COUNT(*) AS view_count
    FROM pageviews
    GROUP BY userid
    EMIT CHANGES;
```

### 6.4 データの挿入

```sql
INSERT INTO pageviews (viewtime, userid, pageid)
VALUES (1612345678901, 'user1', 'page1');
```

### 6.5 よく使う WITH プロパティ

| プロパティ | 説明 |
|------|------|
| `KAFKA_TOPIC` | 裏側の Kafka トピック名 |
| `VALUE_FORMAT` / `KEY_FORMAT` | シリアライズ形式（JSON, AVRO, PROTOBUF, JSON_SR, KAFKA, DELIMITED, NONE） |
| `PARTITIONS` / `REPLICAS` | パーティション数 / レプリカ数 |
| `TIMESTAMP` / `TIMESTAMP_FORMAT` | タイムスタンプのカラムと形式 |
| `WRAP_SINGLE_VALUE` | 単一カラムのときにラップするか（デフォルト true） |

### 6.6 重要な区別：論理オブジェクト名とトピック名、そして PARTITIONS が効くタイミング

**① `FROM pageviews` の `pageviews` は「ストリーム/テーブル名」であり、Kafka トピック名ではありません。**

ksqlDB が扱う対象は論理上のストリーム（STREAM）とテーブル（TABLE）です。各ストリーム/テーブルは `KAFKA_TOPIC` プロパティで裏側の Kafka トピックに対応づけられており、両者は同名でも別名でも構いません。

```sql
-- 論理オブジェクト名 = pageviews、裏側のトピック名 = my_raw_pageviews_topic（別名でもよい）
CREATE STREAM pageviews (...) WITH (
    KAFKA_TOPIC = 'my_raw_pageviews_topic',
    VALUE_FORMAT = 'JSON'
);

-- クエリの FROM に書くのは「ストリーム名」pageviews であり、トピック名ではない
SELECT * FROM pageviews EMIT CHANGES;
```

> 上の 6.3 の例にある `FROM pageviews` / `FROM users` は、先ほど `CREATE STREAM/TABLE` で登録した**ストリーム名 / テーブル名**を使っています。たまたまトピック名と同名（`KAFKA_TOPIC` を省略するとオブジェクト名がそのまま使われる）なので、トピックを書いているように見えますが、実際は別物です。

CSAS / CTAS も同様です。`CREATE STREAM enriched_views AS ...` は**新しいストリームオブジェクト** `enriched_views` を作り、同時に ksqlDB が同名の出力トピックを自動で新規作成します（`WITH (KAFKA_TOPIC=...)` で明示的に名前を変えることも可能）。

**② `PARTITIONS` / `REPLICAS` は、ksqlDB が「トピックを新規作成」するときにだけ効きます。**

`CREATE STREAM/TABLE` の本質は**あるトピックにスキーマを登録すること**であり、無条件にトピックを作るわけではありません。

| 裏側のトピックの状態 | `PARTITIONS` の扱い |
|-----------------|---------------------|
| 既に存在する | 無視される。パーティション数はトピック自身が決めるもので、DDL では変えられない |
| 存在しない | 有効。ksqlDB がこの値で作成する。省略時は Kafka ブローカーのデフォルト `num.partitions` を使う |

そのため、**既に存在するトピック**（特にテーブルは、上流や CDC が作った圧縮ログであることが多い）に接続する場合は `PARTITIONS` を書いても意味がないので省略するのが一般的です。一方、`CREATE TABLE AS SELECT`（CTAS）は ksqlDB が出力トピックを新規作成するため、このときこそ `PARTITIONS` でパーティション数を制御します。

```sql
CREATE TABLE pageviews_per_user
  WITH (PARTITIONS = 6, VALUE_FORMAT = 'JSON')
AS SELECT userid, COUNT(*) AS view_count
   FROM pageviews GROUP BY userid EMIT CHANGES;
```

### 6.7 「登録」と「起動」：オブジェクトの宣言とクエリ実行の違い

`CREATE STREAM/TABLE x (...)` は**スキーマを登録するだけ**で何も処理しません。`AS SELECT` を伴う `CREATE` や `EMIT CHANGES` が**本当にクエリを起動する**ものです。3 種類の文の対応は以下のとおりです。

| 文 | 種類 | 処理が動くか | 新しいオブジェクト / トピックを生むか |
|------|------|--------------|---------------------------|
| `CREATE STREAM pageviews (...)` / `CREATE TABLE users (...)` | 純粋な DDL | ❌ スキーマ登録のみ | 生まない |
| `SELECT ... EMIT CHANGES;` | 一時的なプッシュクエリ | ✅ リアルタイムに読み取り | 生まない（一時的） |
| `CREATE STREAM enriched AS SELECT ...`（CSAS） | 永続クエリ | ✅ 継続的に読み取り→処理→書き込み | ✅ 新ストリーム + 新出力トピック |
| `CREATE TABLE ... AS SELECT ... GROUP BY ...`（CTAS） | 永続クエリ | ✅ 継続的に集約 | ✅ 新テーブル + 新 changelog トピック |
| `SELECT ... WHERE key = ...;`（プルクエリ、テーブルのみ） | 一時的な参照 | 即時返却 | 生まない |

ポイント：

- **登録**（`CREATE ... (...)`）：ksqlDB に「こういうストリーム/テーブルがあって、カラムはこうで、裏側のトピックはこれ」と伝えるだけ。消費も処理もしません。
- **起動**（`AS SELECT` / `EMIT CHANGES`）：Kafka Streams の処理タスクを実際に動かします。
- **ストリームとテーブルの「登録 vs 起動」の考え方はまったく同じ**です。違いは、テーブルは状態（更新可能なスナップショット、キーによるプルクエリ可）を表し、ストリームはイベント（追記のみ）を表す点だけです。

---

## 7. クエリの種類

### 7.1 プッシュクエリ（Push Query）

結果の更新を継続的に配信します。リアルタイムの購読に向いています。

```sql
-- 金額が 100 を超える取引をリアルタイムに表示
SELECT * FROM transactions
WHERE amount > 100
EMIT CHANGES;
```

### 7.2 プルクエリ（Pull Query）

キーで実体化テーブルの現在の状態を参照します（低レイテンシ）。

```sql
-- 特定ユーザーのリアルタイム集計を参照
SELECT * FROM pageviews_per_user
WHERE userid = 'user1';
```

### 7.3 永続クエリ（Persistent Query）

結果を継続的に新しいトピックへ書き込みます（6.3 の CSAS/CTAS を参照）。

---

## 8. 結合（Join）と集約（Aggregation）

### 8.1 対応する Join の種類

| Join | 組み合わせ | 説明 |
|------|------|------|
| Stream-Stream | stream + stream | 時間ウィンドウベース（`WITHIN` の指定が必須） |
| Stream-Table | stream + table | ストリームデータと現在のテーブル状態を結合（テーブルのスナップショット） |
| Table-Table | table + table | 2 つのテーブルをキーで結合し、更新に追随 |
| INNER / LEFT OUTER / FULL OUTER | — | 3 種類の結合セマンティクス |

```sql
-- Stream-Table 結合の例
CREATE STREAM enriched_orders AS
    SELECT o.order_id, o.user_id, u.email
    FROM orders o
    LEFT JOIN users u ON o.user_id = u.user_id
    EMIT CHANGES;
```

### 8.2 集約とウィンドウ（Windowing）

```sql
-- タンブリングウィンドウ（Tumbling）でカウント
CREATE TABLE orders_per_min AS
    SELECT COUNT(*) AS order_count
    FROM orders
    WINDOW TUMBLING (SIZE 1 MINUTE)
    GROUP BY user_id
    EMIT CHANGES;

-- ホッピングウィンドウ（Hopping）
WINDOW HOPPING (SIZE 5 MINUTES, ADVANCE BY 1 MINUTE)

-- セッションウィンドウ（Session）
WINDOW SESSION (1 MINUTE)
```

| ウィンドウの種類 | 説明 |
|----------|------|
| Tumbling | 固定サイズで重なりなし |
| Hopping | 固定サイズで重なりあり |
| Session | アクティビティの間隔に基づいて動的に区切る |

---

## 9. コネクター（Connectors）

ksqlDB には Kafka Connect の統合が組み込まれており、SQL で直接 source/sink コネクターを管理して、外部システムとデータをやり取りできます。

```sql
-- source コネクターを作成（外部システムから Kafka へデータを取り込む）
CREATE SOURCE CONNECTOR mysql_source WITH (
    'connector.class' = 'io.debezium.connector.mysql.MySqlConnector',
    'database.hostname' = 'localhost',
    'database.port' = '3306',
    ...
);

-- sink コネクターを作成（Kafka のデータを外部システムへ書き出す）
CREATE SINK CONNECTOR s3_sink WITH (
    'connector.class' = 'io.confluent.connect.s3.S3SinkConnector',
    ...
);

SHOW CONNECTORS;      -- コネクターを表示
DROP CONNECTOR <name>; -- コネクターを削除
```

---

## 10. カスタム関数（UDF / UDAF / UDTF）

ksqlDB では Java で関数ライブラリを拡張できます。

| 種類 | 説明 | 例 |
|------|------|------|
| **UDF** | スカラー関数。1 行入って 1 行出る | `MASK(email)`, `GEO_DISTANCE(...)` |
| **UDAF** | 集約関数。複数行入って 1 行出る | カスタムの `COLLECT_SET` |
| **UDTF** | テーブル関数。1 行入って複数行出る | `EXPLODE(array)` |

実装方法：Java でアノテーションを付けて実装し、jar に固めて ksqlDB の拡張ディレクトリ（`ksql.extension.dir`）に配置します。

```sql
-- カスタム関数を使う
SELECT MASK(email) AS masked_email FROM users EMIT CHANGES;
```

---

## 11. 主なユースケース

| ユースケース | 説明 |
|------|------|
| **ストリーミング ETL** | データをリアルタイムにクレンジング・変換・エンリッチして Kafka に書き戻す |
| **マテリアライズドビュー** | クリック数・注文数・残高などの指標をリアルタイムに集約し、プルクエリに提供する |
| **リアルタイム監視とアラート** | 異常検知や閾値アラート（IoT センサー、取引の不正検知など） |
| **イベント駆動マイクロサービス** | ストリーミングデータをサービス間通信に使い、リアルタイムに応答する |
| **クリックストリーム分析** | ユーザー行動のリアルタイム分析 |
| **データ統合** | コネクター経由で外部 DB・オブジェクトストレージ・サードパーティシステムと連携する |

---

## 12. ksqlDB と他のストリーム処理基盤の比較

| 観点 | ksqlDB | Kafka Streams | Apache Flink |
|------|--------|---------------|--------------|
| インターフェース | SQL | Java/Scala API | SQL（Flink SQL）+ Java/Scala/Python |
| 学習コスト | 低い（純粋な SQL） | 中程度（プログラミングが必要） | 中程度（SQL またはプログラミング） |
| 表現力 | 中程度（SQL の制約を受ける） | 高い | 高い |
| 状態管理 | RocksDB（自動） | RocksDB（手動設定） | 内蔵の State Backend |
| デプロイ | 独立クラスター / Confluent | アプリケーションに組み込み | 独立クラスター |
| 向いている用途 | 手軽なストリーミング SQL、マテリアライズドビュー | 複雑な独自ロジック | 複雑かつ大規模なストリーム/バッチ統合 |
| エコシステム | Confluent と深く統合 | 純粋な OSS | 独立したエコシステム |

**選定の目安**：
- SQL で手早くストリーム処理・マテリアライズドビュー・リアルタイムアラートを実現したい → **ksqlDB**（特に既に Confluent エコシステム内にいる場合）。
- 複雑な独自ロジックや細かい制御が必要 → **Kafka Streams** または **Flink**。
- **新規プロジェクト**：Confluent の現在の方針に従い、**Flink** を優先的に検討。

---

## 13. ベストプラクティスとよくある制約

### 13.1 ベストプラクティス

1. **ストリームかテーブルかを明確に**：事実（イベント）はストリーム、状態（スナップショット）はテーブルを使う。
2. **Schema Registry を活用**：Avro/Protobuf でスキーマを管理し、進化の互換性を保つ。
3. **パーティションを適切に設計**：結合・集約には同じキーが必要。`PARTITION BY` による再パーティションのコストに注意する。
4. **状態サイズを抑える**：ウィンドウ集約では保持期間（retention）に注意し、RocksDB の無制限な増大を避ける。
5. **監視**：Control Center / JMX / REST API でクエリのレイテンシ・スループット・状態を監視する。
6. **高可用性**：同じ `service.id` で複数ノードをデプロイし、standby replicas を設定する。

### 13.2 よくある制約（はまりやすい点）

- **キーのスキーマを直接変えられない**：CSAS で別ストリームから派生するとき、キー構造を直接変更できません（「Key missing from projection」エラー）。よくある回避策は、一旦フラットな中間ストリームに展開し、`CREATE TABLE AS SELECT ... GROUP BY` の集約でキーを再定義する方法です。
- **Stream-Stream 結合は時間ウィンドウ（`WITHIN`）の指定が必須**です。
- **プルクエリの制約**：実体化テーブル（`CREATE TABLE AS SELECT` が生んだもの）にしか使えず、キーによる照会になります。
- **パーティションの変更**：作成後は直接変更できず、drop して作り直す必要があります。
- **SQL の表現力は限定的**：汎用プログラミングに比べ、複雑なロジック（複雑な状態機械など）は SQL で表現しづらいです。

---

## 14. 参考資料

- [ksqlDB Overview — Confluent 公式ドキュメント](https://docs.confluent.io/platform/current/ksqldb/overview.html)
- [ksqlDB チュートリアルと例](https://docs.confluent.io/platform/8.3/ksqldb/tutorials/overview.html)
- [ksqlDB 構文リファレンス（CREATE STREAM AS SELECT）](https://docs.confluent.io/platform/current/ksqldb/developer-guide/ksqldb-reference/create-stream-as-select.html)
- [ksqlDB 公式ドキュメントサイト](https://docs.ksqldb.io/en/latest/)
- [GitHub オープンソースリポジトリ confluentinc/ksql](https://github.com/confluentinc/ksql)
- [Conduktor — ksqlDB の用語と紹介](https://www.conduktor.io/glossary/ksqldb-for-real-time-data-processing)
- [Confluent Developer — Inside ksqlDB コース](https://developer.confluent.io/inside-ksqldb/)

---

> 注意：本ドキュメントは知識の整理（公式ドキュメントと公開資料に基づく）であり、SQL 例は ksqlDB の一般的な構文に沿ったものです。本番環境では、使用中の Confluent Platform のバージョンに対応する公式ドキュメントを参照してください。
