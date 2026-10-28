# Confluent Platform — Apache Flink® / Flink SQL テクニカルガイド

## 目次

1. [Flink とは](#1-flink-とは)
2. [基本概念](#2-基本概念)
3. [アーキテクチャと動作のしくみ](#3-アーキテクチャと動作のしくみ)
4. [時間とウィンドウ](#4-時間とウィンドウ)
5. [Flink SQL 構文の早見表](#5-flink-sql-構文の早見表)
6. [コネクターとフォーマット](#6-コネクターとフォーマット)
7. [Join の詳細](#7-join-の詳細)
8. [Top-N と重複排除](#8-top-n-と重複排除)
9. [状態と耐障害性](#9-状態と耐障害性)
10. [Flink と ksqlDB の比較](#10-flink-と-ksqldb-の比較)
11. [主なユースケース](#11-主なユースケース)
12. [ベストプラクティスとよくある落とし穴](#12-ベストプラクティスとよくある落とし穴)
13. [参考資料](#13-参考資料)

---

## 1. Flink とは

- **ストリームとバッチの統合（stream-batch unified）**：Flink はバッチ処理を「有界ストリーム」として扱い、同じエンジン・同じ API/SQL でストリームとバッチの両方をサポートします。
- **低レイテンシかつ高スループット**：マイクロバッチではなく、真のレコード単位（record-by-record）のストリーム処理です。
- **ステートフルな計算**：集約・重複排除・Join などのステートフル演算子をサポートし、状態は永続化・復元できます。
- **Exactly-Once（正確に 1 回）**：チェックポイントと二相コミット（2PC）によってエンドツーエンドの正確に 1 回を実現します。
- **多層 API**：SQL / Table API（宣言的）→ DataStream API（関数的）→ ProcessFunction（低レベル）。Flink SQL は最上位の宣言的な入り口であり、本ドキュメントの主役です。

### 1.1 Flink のレイヤー

```
Flink SQL  ──►  Table API  ──►  DataStream API  ──►  ProcessFunction
（宣言的）      （宣言的）        （関数的）           （最下層・最も柔軟）
```

### 1.2 Confluent ディストリビューション

| ディストリビューション | 説明 |
|--------|------|
| **Apache Flink（アップストリーム）** | オープンソースのストリーム・バッチ統合エンジン。本ドキュメントの SQL 構文の土台 |
| **Confluent Platform for Apache Flink** | セルフホスト型。Kafka・Schema Registry・RBAC と深く統合し、`*-confluent` フォーマットを提供 |
| **Flink on Confluent Cloud** | マネージドサービス。Confluent が運用し、Flink Compute Pool や SQL ワークスペースなどを提供 |

> ⚠️ 戦略上の注意：新しいストリーム処理のワークロードについては、現時点で Confluent は Flink（ksqlDB ではなく）を推奨しています。ksqlDB は既存アプリケーションに対しては引き続き完全にサポートされます。両者の比較は[第 10 節](#10-flink-と-ksqldb-の比較)を参照してください。

---

## 2. 基本概念

### 2.1 有界ストリームと無界ストリーム

| 概念 | 説明 | 例 |
|------|------|------|
| **無界ストリーム（Unbounded Stream）** | 始点はあるが終点がない、継続的に発生する | Kafka メッセージ、センサーデータ、クリックストリーム |
| **有界ストリーム（Bounded Stream）** | 始点も終点もある | ファイル、データベーステーブルの全量データ |

バッチ処理＝有界ストリームに対する計算、ストリーム処理＝無界ストリームに対する計算。Flink は統一モデルで両方を扱います。

### 2.2 動的テーブル（Dynamic Table）

Flink SQL の中核となる抽象概念です。従来のリレーショナルテーブルと異なり、**動的テーブルの内容は時間とともに変化し続けます**：

```
ストリーム ──► 動的テーブル ──► 継続的クエリ ──► 動的テーブル ──► ストリーム
```

- 入力ストリームは動的テーブルにマッピングされ、動的テーブルに対するクエリは**継続的クエリ**として新しい結果ストリームを出力し続けます。
- `CREATE TABLE` で動的テーブルを宣言し、`SELECT`/`INSERT` で継続的クエリを定義します。

### 2.3 時間セマンティクス

| 時間の種類 | 説明 | 用途 |
|----------|------|------|
| **イベント時間（Event Time）** | イベントが実際に発生した時刻（データ内に記録） | 業務的に正しい時間。順序の乱れ・遅延データを扱う |
| **処理時間（Processing Time）** | 演算子がそのイベントを処理したマシン時刻 | 性能は最良だが、結果が不確定で再現できない |
| **取り込み時間（Ingestion Time）** | データが Flink に入った時刻（DataStream API のみ） | 両者の中間 |

> 本番の集計系指標はほぼ**イベント時間**を使い、ウォーターマークで順序の乱れを扱います。

### 2.4 ウォーターマーク（Watermark）

イベント時間では順序の乱れが当たり前です。**ウォーターマーク**は「その時刻より前のデータはすべて到着しているはず」という特別なタイムスタンプです：

- 演算子はウォーターマークに基づき、いつウィンドウ計算を発火し、いつ状態を閉じるかを判断します。
- ウォーターマークが「遅い」ほど乱れへの耐性は増しますが、遅延も大きくなります。

### 2.5 状態（State）

ステートフル演算子（集約・Join・重複排除）は中間結果を保持する必要があります。Flink の状態の特徴：

- **ローカル保存**（メモリまたは RocksDB）。**チェックポイント**で定期的に信頼できるストレージへ永続化。
- **キー単位で分割**：状態はキーに沿って各 TaskManager に分散。
- **State TTL** により状態の無制限な増加を防げます。

---

## 3. アーキテクチャと動作のしくみ

### 3.1 マスター／ワーカー構成

| 役割 | 責務 |
|------|------|
| **JobManager（JM）** | 調整役：ジョブ受付、実行グラフ生成、タスクのスケジューリング、チェックポイント調整、障害復旧 |
| **TaskManager（TM）** | 実行役：具体的な演算子タスクを実行し、**タスクスロット**（リソース単位）を管理 |
| **Client** | ジョブの送信、セーブポイントの発火など |

### 3.2 ジョブグラフの 4 層

```
StreamGraph（論理） ──► JobGraph（最適化後・冗長除去） ──► ExecutionGraph（並列度展開） ──► 物理実行グラフ（実際のデプロイ）
```

### 3.3 並列度と演算子チェーン

- **並列度（Parallelism）**：1 つの演算子の並列インスタンス数。演算子／ジョブ／全体の 3 段階で設定できます。
- **演算子チェーン（Operator Chaining）**：隣接し条件を満たす演算子を 1 つのタスクにまとめ、直列化やスレッド切替のオーバーヘッドを削減します。
- **スロット共有（Slot Sharing）**：同一ジョブの異なる演算子が同じスロットを共有し、リソース利用率を高めます。

### 3.4 チェックポイントとセーブポイント

| 観点 | チェックポイント | セーブポイント |
|------|-----------|-----------|
| 発火 | 自動・定期 | 手動 |
| 目的 | 障害復旧 | ジョブのアップグレード、移行、A/B テスト、再起動 |
| 保持 | 自動で消えることがある | 長期保持 |
| 状態の一貫性 | チェックポイントモードに依存 | 全量スナップショット |

### 3.5 状態バックエンド（State Backend）

| State Backend | 状態の置き場所 | 用途 |
|---------------|----------|----------|
| `HashMapStateBackend` | JVM ヒープメモリ | 状態が小さく、低遅延が求められる場合 |
| `EmbeddedRocksDBStateBackend` | ローカル RocksDB | 状態が大きく、メモリを超える場合 |

### 3.6 耐障害性と一貫性

- **再起動戦略**：固定遅延再起動、失敗率ベースの再起動、再起動なし、など。
- **一貫性セマンティクス**：
  - `at-least-once`：重複の可能性あり、性能は最良。
  - `exactly-once`：エンドツーエンドの正確に 1 回。チェックポイント + 二相コミット（2PC）が必要で、**シンクがトランザクション対応**（Kafka トランザクション、JDBC トランザクション）である必要があります。

---

## 4. 時間とウィンドウ

### 4.1 ウォーターマークの宣言

`CREATE TABLE` の DDL でイベント時間列とウォーターマーク生成戦略を宣言します：

```sql
CREATE TABLE clicks (
    user_id BIGINT,
    page_id STRING,
    ts      TIMESTAMP(3),
    -- イベント時間列 ts、5 秒の乱れを許容
    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'clicks',
    'properties.bootstrap.servers' = 'localhost:9092',
    'format' = 'json'
);
```

### 4.2 ウィンドウの種類

#### タンブリングウィンドウ（Tumbling Window）

固定サイズで重なりません。

```sql
SELECT
    user_id,
    window_start,
    window_end,
    COUNT(*) AS cnt
FROM TABLE(
    TUMBLE(TABLE clicks, DESCRIPTOR(ts), INTERVAL '1' MINUTE)
)
GROUP BY window_start, window_end, user_id;
```

#### ホッピングウィンドウ（Hopping Window）

固定サイズでステップごとにスライドします（重なりあり）。

```sql
-- サイズ 10 分、5 分ごとにスライド
SELECT window_start, window_end, COUNT(*)
FROM TABLE(
    HOP(TABLE clicks, DESCRIPTOR(ts), INTERVAL '5' MINUTE, INTERVAL '10' MINUTE)
)
GROUP BY window_start, window_end;
```

#### セッションウィンドウ（Session Window）

イベント間隔に応じて動的に区切ります（ギャップを超えると新しいセッション）。

```sql
SELECT window_start, window_end, COUNT(*)
FROM TABLE(
    SESSION(TABLE clicks, DESCRIPTOR(ts), INTERVAL '5' MINUTE)
)
GROUP BY window_start, window_end;
```

#### 累積ウィンドウ（Cumulate Window）

固定ステップで累積します（ステップごとに累積値を出力）：

```sql
SELECT window_start, window_end, SUM(amount)
FROM TABLE(
    CUMULATE(TABLE orders, DESCRIPTOR(ts), INTERVAL '1' MINUTE, INTERVAL '1' HOUR)
)
GROUP BY window_start, window_end;
```

#### OVER ウィンドウ（行単位のウィンドウ）

グループ化せず、各行に対してウィンドウ範囲の集約を計算します：

```sql
SELECT
    user_id,
    amount,
    SUM(amount) OVER (
        PARTITION BY user_id
        ORDER BY ts
        RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW
    ) AS hourly_sum
FROM orders;
```

### 4.3 ウィンドウの早見表

| ウィンドウ | 特徴 | 主要パラメータ |
|------|------|----------|
| TUMBLE | 固定・重なりなし | size |
| HOP | 固定・重なりあり | size + slide |
| SESSION | ギャップで動的に | gap |
| CUMULATE | 固定ステップで累積 | step + max size |
| OVER | 行単位・グループ化なし | partition + order + range |

### 4.4 遅延データ（Late Data）

ウォーターマークを超えてから到着したイベント＝遅延データです。処理方針：

- **そのまま破棄**（デフォルト）。
- **遅延許容 + サイド出力**：`allowedLateness` を設定すると、遅延データはウィンドウ再計算を発火し、さらに超えた分はサイド出力（side output）へ流して個別に処理できます。

---

## 5. Flink SQL 構文の早見表

### 5.1 データ型

| Flink SQL 型 | 説明 |
|----------------|------|
| `STRING` | 文字列（ksqlDB の `VARCHAR` とは異なる点に注意） |
| `INT` / `BIGINT` / `DOUBLE` / `DECIMAL(p,s)` | 数値 |
| `BOOLEAN` | 真偽値 |
| `TIMESTAMP(3)` / `TIMESTAMP_LTZ(3)` | タイムスタンプ（後者はタイムゾーン付き） |
| `ARRAY<T>` / `MAP<K,V>` / `ROW<...>` | 複合型 |

### 5.2 DDL

```sql
-- データベース作成
CREATE DATABASE my_db;

-- テーブル作成（動的テーブル、外部システムにマッピング）
CREATE TABLE orders (
    order_id  BIGINT,
    user_id   BIGINT,
    amount    DECIMAL(10, 2),
    ts        TIMESTAMP(3),
    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'orders',
    'properties.bootstrap.servers' = 'localhost:9092',
    'format' = 'json'
);

-- ビュー作成（論理テーブル、実体化しない）
CREATE VIEW enriched_orders AS
    SELECT o.*, u.name
    FROM orders o JOIN users u ON o.user_id = u.user_id;

-- 関数作成
CREATE FUNCTION my_udf AS 'com.example.MyUdf';
```

### 5.3 DML：継続的クエリ

```sql
-- 継続ジョブの投入：集約結果をシンクへ書き込む
INSERT INTO user_order_cnt
SELECT user_id, COUNT(*) AS cnt
FROM orders
GROUP BY user_id;

-- テーブル作成と継続書き込み（CTAS）：新しいテーブルを作りつつ書き込みジョブを開始
CREATE TABLE orders_filtered
WITH ('connector' = 'kafka', 'topic' = 'orders_filtered', 'format' = 'json')
AS SELECT * FROM orders WHERE amount > 100;
```

> また `REPLACE TABLE ... AS SELECT`（RTAS）でテーブル全体を原子的に置き換えられます。

### 5.4 SELECT でよく使う句

```sql
SELECT DISTINCT user_id          -- 重複排除
FROM orders
WHERE amount > 100               -- フィルタ
GROUP BY user_id                 -- グループ化
HAVING COUNT(*) > 3              -- グループ化後のフィルタ
ORDER BY cnt DESC                -- ソート
LIMIT 10;                        -- 件数制限
```

### 5.5 集合演算

```sql
SELECT user_id FROM stream_a
UNION      -- UNION ALL / INTERSECT / EXCEPT
SELECT user_id FROM stream_b;
```

---

## 6. コネクターとフォーマット

### 6.1 主なコネクター（Connector）

| コネクター | 識別子 | 説明 |
|--------|------|------|
| Kafka | `'connector' = 'kafka'` | 最も一般的。Kafka ストリームの読み書き |
| Upsert Kafka | `'connector' = 'upsert-kafka'` | upsert セマンティクスで Kafka に書き込む（主キーと組み合わせ、更新/削除に対応） |
| Datagen | `'connector' = 'datagen'` | テストデータを生成。`rows-per-second` で速度制御 |
| Print | `'connector' = 'print'` | 標準出力に表示。デバッグ用 |
| BlackHole | `'connector' = 'blackhole'` | データを破棄。性能テスト用 |
| JDBC | `'connector' = 'jdbc'` | リレーショナル DB の読み書き |
| Filesystem | `'connector' = 'filesystem'` | ファイルの読み書き（HDFS/S3/ローカル） |
| CDC（Debezium/Flink CDC） | `'connector' = 'mysql-cdc'` など | 変更データキャプチャ。DB の binlog/WAL を読む |

### 6.2 主なフォーマット（Format）

| フォーマット | 識別子 | 説明 |
|------|------|------|
| JSON | `'format' = 'json'` | 汎用的 |
| Avro | `'format' = 'avro'` | スキーマ付き、コンパクト |
| Protobuf | `'format' = 'protobuf'` | スキーマ付き、言語横断 |
| CSV | `'format' = 'csv'` | シンプルなテキスト |
| Parquet / ORC | `'format' = 'parquet'` / `'orc'` | 列指向、バッチ/分析向け |
| Debezium-JSON | `'format' = 'debezium-json'` | CDC 変更イベントを解析 |
| Canal-JSON | `'format' = 'canal-json'` | Canal 変更イベントを解析 |

> Confluent ディストリビューションの Flink は `avro-confluent` / `protobuf-confluent` / `json-schema-confluent` フォーマットを追加提供し、Schema Registry と直接統合します（schema id 付きメッセージの読み書き、互換性検証）。

### 6.3 Kafka 読み書きの例

```sql
-- 読み：Kafka → Flink
CREATE TABLE orders (
    order_id BIGINT,
    amount   DECIMAL(10, 2),
    ts       TIMESTAMP(3),
    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'orders',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'flink-orders',
    'format' = 'json',
    'scan.startup.mode' = 'earliest-offset'   -- 先頭から読む。または 'latest-offset'
);

-- 書き：Flink → Kafka（upsert、主キー付きなら更新セマンティクスに対応）
CREATE TABLE user_order_cnt (
    user_id BIGINT,
    cnt     BIGINT,
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'user_order_cnt',
    'properties.bootstrap.servers' = 'localhost:9092',
    'key.format' = 'json',
    'value.format' = 'json'
);
```

### 6.4 主な Kafka 設定項目

| 設定 | 説明 |
|------|------|
| `scan.startup.mode` | 読み始め位置：`earliest-offset` / `latest-offset` / `group-offsets` / `timestamp` |
| `properties.group.id` | コンシューマーグループ |
| `properties.bootstrap.servers` | ブローカーアドレス |
| `key.format` / `value.format` | key / value のシリアライズ形式 |
| `format` | key と value の両方に適用するフォーマット |
| `sink.partitioner` | 書き出しのパーティション戦略（デフォルトは key ハッシュ） |

---

## 7. Join の詳細

Flink SQL は ksqlDB より豊富な Join をサポートします。

### 7.1 通常の Join（Regular Join）

条件で関連付けます。状態はイベントとともに増え続けます（State TTL で掃除できます）：

```sql
SELECT o.order_id, u.name
FROM orders o
JOIN users u ON o.user_id = u.user_id;
```

### 7.2 時間区間 Join（Interval Join）

2 つのストリームを時間区間で関連付けます（ストリーム対ストリームのみ、イベント時間が必要）：

```sql
SELECT o.order_id, p.payment_id
FROM orders o, payments p
WHERE o.order_id = p.order_id
  AND p.ts BETWEEN o.ts - INTERVAL '5' MINUTE AND o.ts + INTERVAL '5' MINUTE;
```

### 7.3 時相 Join（Temporal Join）

#### バージョンテーブル Join（ストリーム ↔ 更新され続けるディメンションテーブル）

CDC で同期された、バージョン（主キー + イベント時間）を持つディメンションテーブル向け：

```sql
-- products はバージョンテーブル（主キー + タイムスタンプ）
SELECT o.order_id, p.name, p.price
FROM orders o
JOIN products FOR SYSTEM_TIME AS OF o.ts AS p
ON o.product_id = p.id;
```

#### ルックアップ Join（Lookup Join、ストリーム ↔ 静的ディメンションテーブル）

JDBC などの外部静的ディメンションテーブル向け：

```sql
SELECT o.order_id, u.name
FROM orders o
JOIN users FOR SYSTEM_TIME AS OF o.proc_time AS u   -- proc_time は処理時間属性
ON o.user_id = u.user_id;
```

### 7.4 Join の早見表

| Join | 入力 | 意味 |
|------|------|------|
| Regular Join | ストリーム↔ストリーム / ストリーム↔テーブル / テーブル↔テーブル | 標準的な関連付け。状態が増える |
| Interval Join | ストリーム↔ストリーム | イベント時間の区間で関連付け |
| Temporal Join（バージョンテーブル） | ストリーム↔バージョンテーブル | イベント時間で過去スナップショットを取得 |
| Lookup Join | ストリーム↔外部ディメンション | 処理時間で外部ディメンションを参照 |

---

## 8. Top-N と重複排除

### 8.1 Top-N

`ROW_NUMBER()` で各グループの上位 N 件を取得します（イベント時間または処理時間ベース）：

```sql
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC) AS rn
    FROM products
)
WHERE rn <= 5;   -- 各 category で価格上位 5 件
```

### 8.2 ウィンドウ Top-N

先にウィンドウで集約し、各ウィンドウ内の Top-N を取得します：

```sql
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY cnt DESC) AS rn
    FROM (
        SELECT user_id, window_start, window_end, COUNT(*) AS cnt
        FROM TABLE(TUMBLE(TABLE clicks, DESCRIPTOR(ts), INTERVAL '1' MINUTE))
        GROUP BY window_start, window_end, user_id
    )
)
WHERE rn <= 10;   -- 毎分クリック数上位 10 ユーザー
```

### 8.3 重複排除（Dedup）

ストリーミング重複排除（各キーの最新 1 件を保持）：

```sql
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY ts DESC) AS rn
    FROM clicks
)
WHERE rn = 1;
```

---

## 9. 状態と耐障害性

### 9.1 状態の分類

| 状態 | 説明 |
|------|------|
| **Keyed State** | キー単位で分割。`GROUP BY` / `PARTITION BY` の後に使用 |
| **Operator State** | 演算子単位で、キーに依存しない（Kafka ソースのオフセットなど） |
| **管理状態（Managed State）** | Flink が管理。直列化・TTL・復元を自動で処理 |

### 9.2 チェックポイント設定

```yaml
# flink-conf.yaml の主要項目
execution.checkpointing.interval: 3min       # チェックポイント間隔
execution.checkpointing.mode: EXACTLY_ONCE   # 一貫性レベル
state.backend.type: rocksdb                  # 状態バックエンド（旧書き方 state.backend は非推奨）
state.checkpoints.dir: file:///tmp/flink-cp  # 保存先
execution.checkpointing.min-pause: 1min      # 2 回のチェックポイントの最小間隔
```

### 9.3 エンドツーエンドの Exactly-Once

- **ソース側**：再読込可能（Kafka オフセット）。
- **Flink 内部**：チェックポイントが状態の一貫性を保証。
- **シンク側**：二相コミット（2PC）。シンクがトランザクション対応（Kafka トランザクション、JDBC トランザクション）である必要があります。

### 9.4 State TTL（状態の有効期限）

状態の無制限な増加を防ぎます：

```sql
-- Table API / SQL では table.exec.state.ttl で全体設定
-- DataStream では状態デスクリプタごとに設定
StateTtlConfig
    .newBuilder(Time.minutes(30))
    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
    .build();
```

---

## 10. Flink と ksqlDB の比較

| 観点 | Flink | ksqlDB |
|------|-------|--------|
| インターフェース | SQL / Table API / DataStream / ProcessFunction の多層 | 純粋な SQL |
| 表現力 | 高（SQL の表現力が高く、コードまで降りられる） | 中（SQL の制約を受ける） |
| 状態管理 | 内蔵 State Backend（HashMap / RocksDB）で制御可能 | RocksDB（Kafka Streams が自動） |
| ストリーム・バッチ統合 | ✅ ネイティブ対応 | ❌ ストリームのみ |
| 時間セマンティクス | イベント時間 / 処理時間 / 取り込み時間、ウォーターマークが柔軟 | イベント時間 / 処理時間（比較的シンプル） |
| Join 能力 | Regular / Interval / Temporal / Lookup の複数種 | Stream-Stream / Stream-Table / Table-Table |
| デプロイ | 独立クラスタ / クラウドマネージド | 独立クラスタ / Confluent |
| エコシステム | 独立エコシステム + Confluent 統合 | Confluent / Kafka と深く連携 |

**選び方**：
- シンプルなストリーミング ETL、マテリアライズドビュー、リアルタイムアラートだけで、すでに Confluent エコシステムにいる → ksqlDB で十分。
- 複雑な Join・ウィンドウ・重複排除・Top-N・ストリーム/バッチ統合・リソース/状態の細かい制御が必要 → **Flink**。

---

## 11. 主なユースケース

| ユースケース | 説明 |
|------|------|
| **リアルタイム ETL** | クレンジング・変換・エンリッチして Kafka / DWH へ書き戻す |
| **リアルタイム指標集約** | 分単位/秒単位の指標（PV、UV、GMV、CTR） |
| **リアルタイムダッシュボード / レポート** | タンブリング・累積ウィンドウでのリアルタイム集計 |
| **リアルタイムリスク管理 / 不正検知** | ルールエンジン、異常検知、行動系列の分析 |
| **リアルタイム推薦** | ユーザー行動特徴のリアルタイム更新、特徴量エンジニアリング |
| **CDC 同期** | データベースの変更を DWH / キャッシュ / 検索へリアルタイム同期 |
| **ストリーム・バッチ統合** | 同じ SQL でオフラインバッチとリアルタイムストリームの両方を処理 |

---

## 12. ベストプラクティスとよくある落とし穴

### 12.1 ベストプラクティス

1. **イベント時間を優先**：業務的に正しい。処理時間はデバッグや時系列に敏感でない場面だけに。
2. **ウォーターマークを適切に**：乱れへの耐性と遅延のトレードオフ。通常 5 秒～1 分。
3. **Upsert-Kafka + 主キーを使いこなす**：結果を書くとき `PRIMARY KEY ... NOT ENFORCED` + upsert-kafka で、下流が最新状態を読めるように。
4. **状態サイズを制御**：Regular Join・重複排除・集約には State TTL を設定し、無制限な増加を防ぐ。
5. **Kafka のコンシューマーグループとオフセットに注意**：`scan.startup.mode` が読み始め位置を決め、ジョブ再起動時は group.id で続きから読む。
6. **大きな状態は RocksDB**：状態がメモリを超えるなら `EmbeddedRocksDBStateBackend` に切替。
7. **監視**：チェックポイント時間、バックプレッシャー、タスクの偏り、状態サイズに注目。

### 12.2 よくある落とし穴

- **`VARCHAR` と `STRING`**：Flink SQL は `STRING`、ksqlDB は `VARCHAR`。混同しやすい。
- **主キーは `NOT ENFORCED` 必須**：Flink SQL の主キーは論理制約で、一意性を実際には検証しない。
- **ウォーターマークがないとイベント時間ウィンドウが使えない**：DDL で `WATERMARK FOR ...` を忘れると、ウィンドウ/イベント時間 Join がエラーになる。
- **Regular Join の状態が増え続ける**：ストリーム対ストリームの Join は状態を積み上げるので、必ず TTL を設定するか Interval/Temporal Join に替える。
- **チェックポイント失敗によるバックプレッシャー**：チェックポイントが頻繁すぎる/状態が大きすぎるとスループットが落ちる。
- **データ偏り**：特定キーのデータ量が過大だと、単一 TaskManager がボトルネックになる（ホットキー）。

---

## 13. 参考資料

- [Apache Flink 公式ドキュメント](https://nightlies.apache.org/flink/flink-docs-stable/)
- [Flink SQL ドキュメント](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/overview/)
- [Flink SQL Client](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sqlclient/)
- [Flink ウィンドウ（Windowing TVF）](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/queries/window-tvf/)
- [Flink Join ドキュメント](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/queries/joins/)
- [Flink コネクターとフォーマット](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/overview/)
- [Flink 状態とチェックポイント](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/stateful-stream-processing/)

---

> 注記：本ドキュメントは知識整理を目的としたものです。Flink SQL の構文は Flink の現行安定版を基準としています。バージョン間で構文の進化（ウィンドウ TVF、Protobuf フォーマット、CTAS/RTAS、`state.backend.type` など）があるため、実際の使用時はご利用の Flink バージョンに対応する公式ドキュメントを参照してください。
