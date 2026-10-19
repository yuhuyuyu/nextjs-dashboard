# Docker での Confluent 構築（cp-all-in-one）

> 2026-10-19 まとめ

## 1. 取得

リポジトリ：`github.com/confluentinc/cp-all-in-one`、ブランチ例 `8.2.0-post`。

**リポジトリ全体（あるいは最低でも `vol/` ディレクトリ）をダウンロードすること。`docker-compose.yml` 単体ではダメ** —— compose は入れ子ディレクトリ `cp-all-in-one/cp-all-in-one/docker-compose.yml` にあります。

```bash
git clone --depth 1 --branch 8.2.0-post https://github.com/confluentinc/cp-all-in-one.git
cd cp-all-in-one/cp-all-in-one
```

## 2. 重要な罠：vol/config がないと prometheus / alertmanager が起動しない

`prometheus`、`alertmanager`、`control-center` は同じ bind mount を共有します：

```yaml
volumes:
  - ./vol/config:/mnt/config
```

これらは起動時に `vol/config/` 内のベースライン設定ファイルを読み込みます：

| ファイル | 役割 |
|---|---|
| `prometheus-generated.yml` | Prometheus 設定（scrape / rule） |
| `alertmanager-generated.yml` | Alertmanager 設定 |
| `recording_rules-generated.yml` | 事前集約用 recording rules |
| `trigger_rules-generated.yml` | C3 UI で設定するトリガールール |
| `web-config-prom.yml` | Prometheus の web 設定（空） |
| `web-config-am.yml` | Alertmanager の web 設定（空） |

これらのファイルはリポジトリに同梱されています。compose のみダウンロードして `vol/` が欠けると、コンテナ起動時に以下のエラーになります：

```
open /mnt/config/prometheus-generated.yml: no such file or directory
```

**この 6 ファイルだけ補う場合**（`cp-all-in-one/cp-all-in-one/` ディレクトリで実行。ブランチ番号は compose と一致させる）：

```bash
mkdir -p vol/config
BASE="https://raw.githubusercontent.com/confluentinc/cp-all-in-one/8.2.0-post/cp-all-in-one/vol/config"
for f in prometheus-generated.yml alertmanager-generated.yml recording_rules-generated.yml trigger_rules-generated.yml web-config-am.yml web-config-prom.yml; do
  curl -fsSL "$BASE/$f" -o "vol/config/$f"
done
```

## 3. 起動

```bash
docker compose up -d
```

## 4. ポートとヘルスチェック

| サービス | アドレス |
|---|---|
| Control Center | http://localhost:9021 |
| Prometheus | http://localhost:9090（`curl localhost:9090/-/healthy`） |
| Alertmanager | http://localhost:9093（`curl localhost:9093/-/healthy`） |
| Schema Registry | http://localhost:8081 |
| Kafka | localhost:9092 |

監視 UI が不要な場合は、コアサービスのみ起動してメモリを節約できます：

```bash
docker compose up -d broker schema-registry connect ksqldb-server rest-proxy
```

## 5. リソースとクリーンアップ

- メモリ：フルスタック（C3 + ksqlDB + Connect + Flink 含む）は ≥8GB 推奨。Docker Desktop の割当が少ないとコンテナが Exit 137 になることがあります。
- 停止：`docker compose down`
- 完全クリーンアップ（ボリューム内のトピックデータも削除）：`docker compose down -v`
