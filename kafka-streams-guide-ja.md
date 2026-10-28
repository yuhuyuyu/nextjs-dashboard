# Confluent Platform — Kafka Streams テクニカルガイド

## 目次

1. [Kafka Streams とは](#1-kafka-streams-とは)
2. [基本概念](#2-基本概念)
3. [アーキテクチャと並列モデル](#3-アーキテクチャと並列モデル)
4. [DSL と Processor API](#4-dsl-と-processor-api)
5. [KStream / KTable / GlobalKTable](#5-kstream--ktable--globalktable)
6. [DSL のよく使う操作早見表](#6-dsl-のよく使う操作早見表)
7. [状態とステートストア](#7-状態とステートストア)
8. [時間とウィンドウ](#8-時間とウィンドウ)
9. [Join の詳細](#9-join-の詳細)
10. [耐障害性と Exactly-Once](#10-耐障害性と-exactly-once)
11. [設定の早見表](#11-設定の早見表)
12. [ksqlDB / Flink との比較](#12-ksqldb--flink-との比較)
13. [ベストプラクティスとよくある落とし穴](#13-ベストプラクティスとよくある落とし穴)
14. [参考資料](#14-参考資料)

---

## 1. Kafka Streams とは

- **アプリケーションに組み込む Java クライアントライブラリ**で、Kafka のプロデューサー／コンシューマー API の上に直接構築され、データ並列・分散調整・耐障害性を自然に得られます。
- 入力は Kafka topic から、出力は Kafka topic へ書き戻し、状態はローカル（デフォルト RocksDB）に保存。独立クラスタは不要です。
- 中核となる抽象は**処理トポロジー（Processor Topology）**：「ストリームプロセッサー（ノード）+ ストリーム／ステートストア（エッジ）」から成る有向グラフです。

### 1.1 主な特徴

| 特徴 | 説明 |
|------|------|
| フレームワークでなくライブラリ | 独立クラスタなし。アプリと一緒に動き、一緒にスケール |
| Exactly-Once（EOS） | Kafka トランザクション（`exactly_once_v2`）ベース |
| ステートフル | 集約/Join などの状態をローカルに保存し、changelog topic で耐障害化 |
| 柔軟なスケーリング | 入力 topic のパーティション数に応じて水平拡張し、インスタンス間で自動再均衡 |
| 時間セマンティクス | イベント時間 + 処理時間。ウィンドウ、遅延データ処理に対応 |

---

## 2. 基本概念

| 概念 | 説明 |
|------|------|
| **Topology（トポロジー）** | 処理ロジックのグラフ構造：プロセッサーがノード、ストリーム／ステートストアがエッジ |
| **Source Processor** | 上流のないプロセッサー。Kafka topic からレコードを消費して入力とする |
| **Sink Processor** | 下流のないプロセッサー。レコードを指定の Kafka topic に書き戻す |
| **Stream Task（タスク）** | 並列処理の最小単位。各 task がいくつかの入力パーティションを処理 |
| **Stream Thread** | アプリ内の実行スレッド。1 つのスレッドが複数の task を実行可能 |
| **State Store（ステートストア）** | ローカル状態（デフォルト RocksDB）。changelog topic にバックアップ |

---

## 3. アーキテクチャと並列モデル

```
アプリインスタンス（JVM）
 └─ Stream Thread ── Stream Task（パーティション 0、1）── processor topology
      └─ Stream Thread ── Stream Task（パーティション 2、3）── processor topology
```

- **パーティション → タスク**：各入力 topic パーティションが 1 つの task に対応。task は固定の並列単位で、パーティションから task への割り当ては**決して変わらない**。
- **最大並列度 = 入力 topic のパーティション数**：入力 topic が 5 パーティションなら最大 5 インスタンス（余剰インスタンスは待機し、障害時の引き継ぎに使われる）。
- **Sub-topology（サブグラフ）**：1 つのトポロジーは、状態を共有せず topic 経由でデータをやり取りする複数のサブグラフに分割でき、それぞれ独立した task が処理して並列度をさらに拡張できる。
- **再均衡**：あるインスタンスが停止すると、その task は別インスタンスへ自動移行し、同じパーティションから消費を続行する。

> Kafka Streams は**リソースマネージャーではありません**。アプリと一緒に動くライブラリで、タスク割り当てはライブラリが自動で行います。

---

## 4. DSL と Processor API

| API | 階層 | 適用場面 |
|-----|------|------|
| **DSL（推奨）** | 高レベル・宣言的 | ほぼすべての場面：フィルタ／マップ／集約／Join／ウィンドウ |
| **Processor API** | 低レベル・命令的 | 細かな制御が必要な場合（カスタムトポロジー、タイマー、レコードメタデータへのアクセス） |

```java
// DSL の例：ユーザーごとの注文数を集計
StreamsBuilder builder = new StreamsBuilder();
KStream<String, Order> orders = builder.stream("orders");
KTable<String, Long> cnt = orders
    .groupByKey()
    .count();
cnt.toStream().to("user-order-count");
```

```java
// Processor API の例：カスタムトポロジー
Topology topo = new Topology();
topo.addSource("source", "orders")
    .addProcessor("myProc", MyProcessor::new, "source")
    .addSink("sink", "output", "myProc");
```

---

## 5. KStream / KTable / GlobalKTable

| 抽象 | 意味 | たとえ |
|------|------|------|
| **KStream** | 追記型のイベントストリーム。各レコードが独立したイベント | イベントログ |
| **KTable** | 変更ストリーム。各 key の最新値だけを保持（upsert） | テーブル / changelog |
| **GlobalKTable** | **各インスタンス**に全量複製されるディメンションテーブル。静的参照用 | グローバルディメンションテーブル |

```java
// ストリーム（イベントログ） vs テーブル（最新状態）
KStream<String, String> stream = builder.stream("clicks");        // クリックごとに 1 件
KTable<String, String> table  = builder.table("user-profile");    // ユーザーごとの最新プロフィール
GlobalKTable<String, String> g = builder.globalTable("regions"); // 全量ディメンションテーブル
```

---

## 6. DSL のよく使う操作早見表

### 6.1 ステートレス操作

| 操作 | 役割 |
|------|------|
| `map` / `mapValues` | key/value を変換 |
| `filter` / `filterNot` | フィルタ |
| `flatMap` / `flatMapValues` | 1 対多に展開 |
| `selectKey` | key を変更（再パーティションを誘発） |
| `branch` | 条件で複数のストリームに分岐 |
| `merge` | 複数のストリームを結合 |
| `peek` / `foreach` | 副作用（デバッグ／外部送信） |

```java
KStream<String, Order> filtered = orders
    .filter((k, v) -> v.amount > 100)
    .mapValues(v -> v.normalize());
```

### 6.2 ステートフル操作（集約）

| 操作 | 役割 |
|------|------|
| `groupBy` / `groupByKey` | グループ化（`KGroupedStream` を返す） |
| `count` | カウント |
| `reduce` | 縮約 |
| `aggregate` | カスタム集約（初期化器 + 累積器） |
| `windowedBy` | ウィンドウを付与 |
| `suppress` | 中間結果を抑止し、最終的なウィンドウ結果だけを出力 |

```java
KTable<Windowed<String>, Long> hourlyCnt = orders
    .groupByKey()
    .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofHours(1)))
    .count();
```

---

## 7. 状態とステートストア

- ステートフル演算子（`count`/`reduce`/`aggregate`/Join）はローカル状態が必要で、デフォルトは **RocksDB**（in-memory も可）。
- 状態は **changelog topic** で Kafka にバックアップされ、インスタンス障害後は changelog から復元します。
- `Materialized` でストア名・SerDes をカスタムし、**Interactive Query（対話的クエリ）** を有効にして外部から読み取れます。

```java
Materialized<String, Long, KeyValueStore<Bytes, byte[]>> as("user-count-store");
// ストア名で対話的クエリ
ReadOnlyKeyValueStore<String, Long> store =
    streams.store(StoreQueryParameters.fromNameAndType("user-count-store", QueryableStoreTypes.keyValueStore()));
```

| 概念 | 説明 |
|------|------|
| **RocksDB** | デフォルトの状態バックエンド。状態がメモリを超えられる |
| **changelog topic** | 状態の変更ログ。耐障害性のための復元に使う |
| **state.dir** | ローカル状態ディレクトリ（デフォルト `/tmp/kafka-streams`） |
| **Interactive Query** | `KafkaStreams.store(...)` で状態を点照会 |

---

## 8. 時間とウィンドウ

### 8.1 時間セマンティクス

| 時間 | 説明 |
|------|------|
| **イベント時間（Event Time）** | レコードに埋め込まれたタイムスタンプ。業務的な意味で正しく、ウィンドウ／遅延処理と併用 |
| **処理時間（Processing Time）** | 処理時のマシン時刻。性能は最良だが結果が不確定 |

### 8.2 ウィンドウの種類

| ウィンドウ | API | 説明 |
|------|-----|------|
| タンブリング | `TimeWindows.ofSizeWithNoGrace(size)` | 固定・重ならない |
| ホッピング | `TimeWindows.ofSizeAndGrace(size, grace).advanceBy(step)` | 固定・ステップでスライド |
| スライディング | `SlidingWindows.ofTimeDifferenceAndGrace(maxDiff, grace)` | レコードの時間差で（Join のみ） |
| セッション | `SessionWindows.ofInactivityGapAndGrace(gap, grace)` | 非アクティブ間隔で動的に区切る |

```java
// 5 分のタンブリングウィンドウ、1 分の遅延（grace）を許容
TimeWindows.ofSizeAndGrace(Duration.ofMinutes(5), Duration.ofMinutes(1));
```

- **grace**：ウィンドウを閉じた後も遅延データを受け付ける時間の余裕。grace を超えた遅延データはデフォルトで破棄されます。

---

## 9. Join の詳細

| Join の種類 | 必要なもの | 意味 |
|-----------|------|------|
| **KStream ↔ KStream** | ウィンドウ（時間区間） | 2 つのストリームを時間ウィンドウで関連付け |
| **KStream ↔ KTable** | なし（テーブルは実体化済み） | ストリームイベントをテーブルでエンリッチ |
| **KTable ↔ KTable** | なし | 2 つのテーブルを key で結合 |
| **KStream ↔ GlobalKTable** | なし（再パーティション不要） | 全量ディメンションテーブルの静的参照 |

```java
// ストリーム-ストリームのウィンドウ Join
KStream<String, Order> joined = orders.join(payments,
    (o, p) -> o.withPayment(p),
    JoinWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(5), Duration.ofMinutes(1)));

// ストリーム-テーブル Join（エンリッチ）
KStream<String, Order> enriched = orders.join(userTable,
    (o, u) -> o.withUserName(u.name));
```

> 注意：KStream と KTable/GlobalKTable の Join は **key 型が同じ**である必要があります。KStream↔KStream の Join はイベント時間 + ウィンドウが必要です。

---

## 10. 耐障害性と Exactly-Once

| 設定 | 説明 |
|------|------|
| `processing.guarantee=at_least_once` | デフォルト。少なくとも 1 回 |
| `processing.guarantee=exactly_once_v2` | Exactly-Once（推奨、Kafka トランザクション ベース） |

- **EOS の原理**：べき等プロデューサー + トランザクション + `read_committed` コンシューマーにより、「消費→処理→書き戻し」をエンドツーエンドで正確に 1 回保証します。
- EOS を有効にすると内部で自動的にトランザクションを使い、状態もトランザクションに合わせて原子的にコミットされます。

```properties
processing.guarantee=exactly_once_v2
```

---

## 11. 設定の早見表

```properties
application.id=orders-streams-app      # 必須。アプリの一意な識別子（消費グループ名/changelog 接頭辞を兼ねる）
bootstrap.servers=localhost:9092
default.key.serde=org.apache.kafka.common.serialization.Serdes$StringSerde
default.value.serde=org.apache.kafka.common.serialization.Serdes$StringSerde
num.stream.threads=4                    # インスタンスごとのスレッド数
state.dir=/var/lib/kafka-streams        # ローカル状態ディレクトリ
processing.guarantee=exactly_once_v2    # 一貫性レベル
cache.max.bytes.buffering=10485760      # 状態キャッシュ（下流への書き込み増幅を抑制）
commit.interval.ms=30000                # コミット間隔
```

| 設定 | 説明 |
|------|------|
| `application.id` | 必須・一意。消費グループと changelog の接頭辞を決める |
| `num.stream.threads` | インスタンスごとのスレッド数（単一インスタンスの並列度を上げる） |
| `default.key/value.serde` | デフォルトのシリアライザー |
| `processing.guarantee` | `at_least_once` / `exactly_once_v2` |
| `state.dir` | ローカル状態ディレクトリ |
| `cache.max.bytes.buffering` | 書き込みキャッシュサイズ（下流レコード数と復元粒度に影響） |

---

## 12. ksqlDB / Flink との比較

| 観点 | Kafka Streams | ksqlDB | Flink |
|------|---------------|--------|-------|
| 形態 | 組み込み **Java ライブラリ** | 独立クラスタ、純 SQL | 独立エンジン、SQL/DataStream |
| インターフェース | DSL / Processor API（Java） | SQL | SQL / Table API / DataStream |
| デプロイ | アプリと一緒に | 独立サービス | 独立クラスタ / クラウドマネージド |
| 状態 | ローカル RocksDB + changelog | RocksDB（内部は Kafka Streams） | State Backend（HashMap/RocksDB） |
| ストリーム・バッチ統合 | ❌ ストリームのみ | ❌ ストリームのみ | ✅ |
| 適用場面 | Java アプリ組み込み、複雑なロジックをコードで制御 | 純 SQL、素早く構築 | 大規模・複雑、ストリーム・バッチ統合 |

**選び方**：既存の Java アプリがあり、組み込みたい・ロジックが複雑 → **Kafka Streams**。SQL を書くだけで済ませたい・すでに Confluent エコシステムにいる → **ksqlDB**。大規模・複雑な Join/ウィンドウ・ストリームバッチ統合が必要 → **Flink**。

---

## 13. ベストプラクティスとよくある落とし穴

**ベストプラクティス**

1. **`application.id` は安定して一意に**：変更すると消費グループ/changelog がリセットされ、再計算や状態喪失が起きうる。
2. **key 型を統一**：`groupByKey` / Join は同じ key 型に依存する。違うとコンパイル時にエラー。
3. **`exactly_once_v2` を使う**：本番レベルの一貫性としてのデフォルト選択。代償はわずかな遅延。
4. **状態の増加を制御**：ウィンドウ集約は grace、セッションは gap を設定し、状態の無限増加を防ぐ。
5. **`num.stream.threads` を適切に**：通常は単一インスタンスのコア数。総 task 数 = インスタンス数 × スレッド数で、入力パーティション数が上限。
6. **対話的クエリに HTTP 層を追加**：Kafka Streams は `store(...)` API しか提供しないため、外部公開には自前の REST エンドポイントが必要。

**よくある落とし穴**

- **`application.id` の変更**：別の消費グループ/状態になったのと同じで、履歴状態が読めない。
- **SerDes の不一致**：`default.*.serde` の設定漏れや型の誤りで、実行時に `ClassCastException`。
- **KStream↔KStream Join のウィンドウ指定漏れ**：ストリーム-ストリーム Join は時間ウィンドウが必須。なければエラー。
- **パーティション数不足**：入力パーティション数を超えてインスタンスを増やしても、余剰分は遊休。先に topic のパーティションを増やす。
- **`selectKey`/`groupBy` による再パーティション**：key をまたぐ操作は中間の repartition topic に書くため、データ量に注意。
- **Scala API の削除**：Kafka 4.x から Kafka Streams は Java API が中心で、Scala DSL は非推奨／削除済み。

---

## 14. 参考資料

- [Kafka Streams 公式ドキュメント](https://docs.confluent.io/platform/current/streams/index.html)
- [Kafka Streams アーキテクチャ](https://docs.confluent.io/platform/current/streams/architecture.html)
- [Kafka Streams DSL ドキュメント](https://docs.confluent.io/platform/current/streams/developer-guide/dsl.html)
- [Processor API ドキュメント](https://docs.confluent.io/platform/current/streams/developer-guide/processor-api.html)

---

> 注記：本ドキュメントは Confluent Platform 8.3（Kafka 4.3）の公式ドキュメントに基づいて整理したものです。DSL / ウィンドウ / EOS の詳細はバージョンによって進化します（例：Kafka 4.x での Scala API 削除）。実際の使用時はご利用のバージョンに対応する公式ドキュメントを参照してください。
