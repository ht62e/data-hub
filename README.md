# 製造装置データ・ハブ

ASP.NET Core / .NET 10で、装置ログの取得を共通APIにまとめる実装です。MSAプラットフォーム上のハブ、JSONファイル／PostgreSQLプロバイダ、内部利用向けgRPC SDK、独自基盤に差し替える通信スケルトンを含みます。

- [配置パターン・通信層・SDK・外部ノード設定](docs/PLATFORM.md)
- [静的DAGによるハブ連結](docs/FEDERATION.md)
- [gRPC通信契約](src/Contracts/hub.proto)
- [設計整理・検証・残課題](docs/REVIEW.md)

## 通信モデル

すべてのハブはMSA上に置きます。クライアントは選択したソースIDを保持し、取得のたびに入口ハブへ送ります。取得経路はプロバイダの配置から決まります。

| クライアント | プロバイダ | 取得経路 |
| --- | --- | --- |
| MSA・gRPC SDK | MSA | 入口ハブ → MSA経由でプロバイダ |
| MSA・gRPC SDK | 外部ノード | 入口ハブ → MSA経由で登録先ハブ → 外部ノード |
| 外部・REST | MSA | 入口ハブ → MSA経由でプロバイダ |
| 外部・REST | 外部ノード | 入口ハブ → MSA経由で登録先ハブ → 外部ノード |

外部ノードは登録先ハブ／そのプラットフォームからのみ到達できれば利用できます。同じプラットフォーム内のSDKでも外部ノードは登録先ハブ経由とし、到達性の自動判定や障害時の経路切り替えは行いません。入口と登録先が同一ハブならハブ間の通信は省きます。外部RESTクライアントへのリダイレクトはありません。

DAGはソースの公開・探索を管理します。外部ノードの取得でもDAG上の中間ハブを順番に通らず、MSA通信で登録先ハブへ直接渡します。登録先ハブの中継RPCはローカル登録だけを参照し、別ハブへ再転送しません。

初回にDAG全体を検証した後は、一部のハブが停止しても確認済みの健全な分岐を利用できます。探索RPCの送信元は、自身が検証した直下の相手の起動識別子を照合します。複数段では、中間ハブの再検証が入口ハブより先に完了し、再起動した孫ハブが入口から見える場合があります。ハブの取得期限は `Read.TimeoutSeconds`、DAG探索の期限は `Federation.DiscoveryTimeoutSeconds` で個別に設定します。[DAGの状態と可用性](docs/FEDERATION.md)

プロバイダは起動後に登録し、リースを更新します。登録はメモリ内で、ハブの再起動後にプロバイダが再登録します。SDKの選択状態はSDKインスタンス内に保持し、RESTクライアントは取得時に選択したIDを指定します。

APIキー、プロバイダの登録内容・登録先、ハブのリース秒数は起動時に確定します。これらの設定変更にはプロセスの再起動が必要です。不正なソースID・表示名や登録先の対応表不足は、登録を繰り返すのではなく起動時のエラーとして扱います。

有効なソースIDは同じ接続先からのみ更新できます。別のMSAサービス／外部ノードからの登録はAlreadyExistsとして拒否します。接続先を変更する場合は、元のプロバイダを停止してリースの失効を待ちます。

## 起動とREST

.NET 10 SDKでビルド後、別々のターミナルで起動します。

```powershell
dotnet build DataHub.slnx -m:1
dotnet run --project src/Hub/Hub.csproj --no-build
```

```powershell
dotnet run --project src/Provider/Provider.csproj --no-build
```

既定ポートはハブRESTが5000、ハブgRPCが5002、プロバイダgRPCが5003です。プロバイダ自身のREST APIはありません。

## コーディング規約

C#の手書きソースは[Google C# Style Guide](https://google.github.io/styleguide/csharpguide.html)に統一しています。リポジトリ直下の`.editorconfig`を正規の設定とし、2スペースインデント、100桁制限、常時波括弧、命名規則（privateメンバーは`_camelCase`）を定義しています。整形は次で実行できます。

```powershell
dotnet format DataHub.slnx whitespace --no-restore
```

生成コード（`obj`配下）は対象外です。

```powershell
$headers = @{ 'x-api-key' = 'development-only-change-me' }
Invoke-RestMethod http://localhost:5000/api/sources -Headers $headers
$sourceId = 'hub-local/file-press-01'
Invoke-RestMethod "http://localhost:5000/api/records?sourceId=$sourceId&limit=1" -Headers $headers
```

ソースIDはローカル／遠隔とも `ハブID/ローカルソースID` です。`GET /api/sources` は一覧と探索状態をまとめて返します。

```json
{
  "sources": [{ "id": "hub-local/file-press-01", "displayName": "Press 01 sample file" }],
  "unavailableHubs": [],
  "truncated": false
}
```

`unavailableHubs` が空でなければ下流の探索失敗・未検証があります。`truncated` はホップ数または件数による打ち切りです。`GET /api/records` は `sourceId` と任意の `limit` を受け取ります。ソースの存在とポリシーは取得時に確認します。

## PostgreSQL

[sql/postgres.sql](sql/postgres.sql) をPostgreSQLに適用し、専用の読み取りアカウントを使用します。

```powershell
$env:SourceType = 'Postgres'
$env:SourceId = 'postgres-press-02'
$env:DisplayName = 'Press 02 PostgreSQL'
$env:ConnectionString = 'Host=localhost;Database=datahub;Username=datahub;Password=change-me'
dotnet run --project src/Provider/Provider.csproj --no-build
```

ファイル版と同時起動する場合は追加プロセスの `Kestrel__Endpoints__Grpc__Url` と `Platform__ServiceId` を変え、ハブの `Platform.Services` にそのプロバイダの対応表を追加します。SDKには入口ハブの対応表を設定します。外部ノードとして置く場合は [外部ノード設定](docs/PLATFORM.md) を参照してください。今回のRDBサンプルはPostgreSQLです。

## 負荷制御

すべての取得はプロバイダの `ReadLimiter` を通ります。同時実行数1、待機は既定32件まで、開始間隔は `MinIntervalMs` 以上、件数は `MaxRows` 以下（ハード上限1,000）です。PostgreSQLはパラメーター化した `LIMIT` と時刻・IDのインデックス、ファイルは10 MiB以下のJSON配列を使用します。両アダプタはレコードを逐次返し、読込中に応答サイズを検査します。

| 設定先 | 項目 | 既定値 |
| --- | --- | --- |
| プロバイダ | `MaxPendingReads` | 32件（0〜1,024、実行中の1件を除く） |
| プロバイダ | `DataLimits.MaxRecordBytes` | 262,144バイト（256 KiB） |
| プロバイダ | `DataLimits.MaxResponseBytes` | 2,097,152バイト（2 MiB） |

バイト上限はprotobufにシリアライズしたサイズを基準にします。レコードごとに `MaxRecordBytes`、相関IDと各レコードのタグ・長さを含む応答全体に `MaxResponseBytes` を適用します。MaxRecordBytesは128以上、MaxResponseBytesは256以上で、レコード上限≦応答上限≦共通のgRPC上限4 MiBとします。超過時は部分的な成功データを返さず、件数を減らすか元データを修正して再要求します。RESTのJSON応答はエスケープによりprotobufサイズより大きくなるため、実際のJSONバイト数がこの上限を超える場合があります。

待機件数の超過はgRPC ResourceExhausted／REST 429、サイズ超過はgRPC ResourceExhausted／REST 422です。中継先でも分類を保持します。未登録・リース期限切れのソースはNotFound／REST 404となります。

`ILoadMonitor` のHTTP実装は `MetricsUrl` から `{ "cpuPercent": 70, "diskPercent": 80 }` の0〜100の値を取得します。高い方が50%を超えると間隔を延ばし、90%以上で `MaxIntervalMs` にします。URL未設定なら固定の最小間隔、設定済みで取得・検証失敗なら最大間隔です。

制御はプロバイダのプロセス単位です。監視値の対象・意味・鮮度、同じDBへの複数プロバイダの合算負荷、ページング、増分取得は今後の設計対象です。`LIMIT` 自体はDBのCPU・I/Oの上限を保証しません。

## 検証と現在の範囲

```powershell
dotnet tests/Regression/bin/Debug/net10.0/Regression.dll
dotnet tests/Smoke/bin/Debug/net10.0/Smoke.dll
dotnet tests/FederationSmoke/bin/Debug/net10.0/FederationSmoke.dll
```

結合テストはハブとファイルプロバイダを起動・終了し、4パターンを単一ハブ・連結ハブの両方で確認します。DAG検証、循環拒否、宛先取り違えの拒否、認証分離、障害・復旧、リース期限切れ、取得間隔も検証します。

MSAの独自名前解決・ゲートウェイはダミーの差し替え対象です。実際のPostgreSQLとMSA環境、ネットワーク分離は別途結合試験が必要です。利用者単位の認可・委譲、署名付き取得許可、監査の永続化は指定どおり未実装です。共有キーとHTTPは開発用設定です。
