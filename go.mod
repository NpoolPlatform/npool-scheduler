module github.com/NpoolPlatform/npool-scheduler

go 1.17

require (
	entgo.io/ent v0.12.0
	github.com/NpoolPlatform/account-middleware v0.0.0-20240529100121-6c5edd373863
	github.com/NpoolPlatform/appuser-middleware v0.0.0-20240119021359-948c8504d662
	github.com/NpoolPlatform/basal-middleware v0.0.0-20231015112137-254853c60eec
	github.com/NpoolPlatform/chain-middleware v0.0.0-20240206054529-d5a31563da6c
	github.com/NpoolPlatform/dtm-cluster v0.0.0-20231011071916-859e5dcbf626
	github.com/NpoolPlatform/g11n-middleware v0.0.0-20231026021135-ec3cd368fc24
	github.com/NpoolPlatform/go-service-framework v0.0.0-20240510075442-89278cb5cf47
	github.com/NpoolPlatform/good-middleware v0.0.0-20240530034806-9d5a98d6f3a6
	github.com/NpoolPlatform/inspire-middleware v0.0.0-20240528041556-261e6c23430a
	github.com/NpoolPlatform/ledger-gateway v0.0.0-20240304032258-18c040f653e0
	github.com/NpoolPlatform/ledger-middleware v0.0.0-20240304030403-1e129dfe4e3f
	github.com/NpoolPlatform/libent-cruder v0.0.0-20240514082633-598d5fc7b1e3
	github.com/NpoolPlatform/message v0.0.0-20240617021145-94fb1d39c25e
	github.com/NpoolPlatform/notif-middleware v0.0.0-20240530112808-1906674219b1
	github.com/NpoolPlatform/order-middleware v0.0.0-20240617022557-b7bbd05961f9
	github.com/NpoolPlatform/review-middleware v0.0.0-20240108100223-106962e5a9c2
	github.com/NpoolPlatform/sphinx-proxy v0.0.0-20231201062049-852b1487d4a9
	github.com/NpoolPlatform/third-middleware v0.0.0-20231011073243-59e4e2a0a8ac
	github.com/dtm-labs/dtm v1.17.3
	github.com/go-redis/redis/v8 v8.11.5
	github.com/google/uuid v1.3.0
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.15.2
	github.com/shopspring/decimal v1.3.1
	github.com/urfave/cli/v2 v2.17.2-0.20221006022127-8f469abc00aa
	google.golang.org/grpc v1.55.0
	google.golang.org/protobuf v1.30.0
)

require go.opentelemetry.io/otel v1.14.0 // indirect

require (
	ariga.io/atlas v0.10.0 // indirect
	github.com/Shonminh/apollo-client v0.4.0 // indirect
	github.com/ThreeDotsLabs/watermill v1.2.0 // indirect
	github.com/ThreeDotsLabs/watermill-amqp/v2 v2.0.7 // indirect
	github.com/agext/levenshtein v1.2.1 // indirect
	github.com/andres-erbsen/clock v0.0.0-20160526145045-9e14626cd129 // indirect
	github.com/apparentlymart/go-textseg/v13 v13.0.0 // indirect
	github.com/armon/go-metrics v0.4.1 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cenkalti/backoff/v3 v3.2.2 // indirect
	github.com/cespare/xxhash v1.1.0 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/common-nighthawk/go-figure v0.0.0-20210622060536-734e95fb86be // indirect
	github.com/coocood/freecache v1.0.1 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.2 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/dtm-labs/dtmdriver v0.0.6 // indirect
	github.com/dtm-labs/logger v0.0.2 // indirect
	github.com/fatih/color v1.14.1 // indirect
	github.com/fsnotify/fsnotify v1.6.0 // indirect
	github.com/go-chassis/go-archaius v1.5.3 // indirect
	github.com/go-chassis/openlog v1.1.3 // indirect
	github.com/go-logr/logr v1.2.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-openapi/inflect v0.19.0 // indirect
	github.com/go-resty/resty/v2 v2.7.0 // indirect
	github.com/go-sql-driver/mysql v1.7.0 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/go-cmp v0.5.9 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0 // indirect
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0 // indirect
	github.com/hashicorp/consul/api v1.19.1 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-cleanhttp v0.5.2 // indirect
	github.com/hashicorp/go-hclog v1.4.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-rootcerts v1.0.2 // indirect
	github.com/hashicorp/golang-lru v0.5.5-0.20210104140557-80c98217689d // indirect
	github.com/hashicorp/hcl v1.0.0 // indirect
	github.com/hashicorp/hcl/v2 v2.13.0 // indirect
	github.com/hashicorp/serf v0.10.1 // indirect
	github.com/klauspost/compress v1.15.15 // indirect
	github.com/lithammer/shortuuid/v3 v3.0.7 // indirect
	github.com/magiconair/properties v1.8.6 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.17 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.4 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/mitchellh/go-wordwrap v1.0.0 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/montanaflynn/stats v0.6.6 // indirect
	github.com/natefinch/lumberjack v2.0.0+incompatible // indirect
	github.com/oklog/ulid v1.3.1 // indirect
	github.com/pelletier/go-toml v1.9.5 // indirect
	github.com/pelletier/go-toml/v2 v2.0.7 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/prometheus/client_golang v1.14.0 // indirect
	github.com/prometheus/client_model v0.3.0 // indirect
	github.com/prometheus/common v0.42.0 // indirect
	github.com/prometheus/procfs v0.9.0 // indirect
	github.com/rabbitmq/amqp091-go v1.2.0 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/spf13/afero v1.9.2 // indirect
	github.com/spf13/cast v1.5.0 // indirect
	github.com/spf13/jwalterweatherman v1.1.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/spf13/viper v1.12.0 // indirect
	github.com/subosito/gotenv v1.3.0 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.1.2 // indirect
	github.com/xdg-go/stringprep v1.0.4 // indirect
	github.com/xrash/smetrics v0.0.0-20201216005158-039620a65673 // indirect
	github.com/youmark/pkcs8 v0.0.0-20181117223130-1be2e3e5546d // indirect
	github.com/zclconf/go-cty v1.8.0 // indirect
	go.mongodb.org/mongo-driver v1.13.1 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.31.0 // indirect
	go.opentelemetry.io/otel/exporters/jaeger v1.6.3 // indirect
	go.opentelemetry.io/otel/sdk v1.6.3 // indirect
	go.opentelemetry.io/otel/trace v1.14.0 // indirect
	go.uber.org/atomic v1.10.0 // indirect
	go.uber.org/multierr v1.8.0 // indirect
	go.uber.org/ratelimit v0.2.0 // indirect
	go.uber.org/zap v1.24.0 // indirect
	golang.org/x/crypto v0.4.0 // indirect
	golang.org/x/mod v0.9.0 // indirect
	golang.org/x/net v0.8.0 // indirect
	golang.org/x/sync v0.1.0 // indirect
	golang.org/x/sys v0.6.0 // indirect
	golang.org/x/text v0.8.0 // indirect
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2 // indirect
	google.golang.org/genproto v0.0.0-20230306155012-7f2fa6fef1f4 // indirect
	gopkg.in/ini.v1 v1.66.6 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace google.golang.org/grpc => github.com/grpc/grpc-go v1.41.0

replace entgo.io/ent => entgo.io/ent v0.11.2

replace ariga.io/atlas => ariga.io/atlas v0.5.1-0.20220717122844-8593d7eb1a8e

replace github.com/ugorji/go => github.com/ugorji/go v0.0.0-20190204201341-e444a5086c43
