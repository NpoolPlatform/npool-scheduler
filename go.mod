module github.com/NpoolPlatform/staker-manager

go 1.16

require (
	entgo.io/ent v0.11.1
	github.com/NpoolPlatform/api-manager v0.0.0-20220205130236-69d286e72dba
	github.com/NpoolPlatform/appuser-manager v0.0.0-20220210093932-4b9db1361d89
	github.com/NpoolPlatform/cloud-hashing-billing v0.0.0-20220723051558-cba48d2c1641
	github.com/NpoolPlatform/cloud-hashing-goods v0.0.0-20220224053549-7b30ca7c2e28
	github.com/NpoolPlatform/cloud-hashing-order v0.0.0-20220225132002-fbc8b850fb8c
	github.com/NpoolPlatform/go-service-framework v0.0.0-20220723040805-27af9fb6d458
	github.com/NpoolPlatform/libent-cruder v0.0.0-20220526050249-956b54fac9f1
	github.com/NpoolPlatform/message v0.0.0-20220714075917-2ba73782c509
	github.com/NpoolPlatform/oracle-manager v0.0.0-20220615040511-dc235b31fa14
	github.com/NpoolPlatform/sphinx-coininfo v0.0.0-20211208035009-5ad2768d2290
	github.com/NpoolPlatform/sphinx-proxy v0.0.0-20211209140052-de1778e36e36
	github.com/NpoolPlatform/stock-manager v0.0.0-20220501144918-883eece82647
	github.com/go-resty/resty/v2 v2.7.0
	github.com/google/uuid v1.3.0
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.8.0
	github.com/kr/pretty v0.3.0 // indirect
	github.com/rogpeppe/go-internal v1.8.1-0.20211023094830-115ce09fd6b4 // indirect
	github.com/streadway/amqp v1.0.0
	github.com/stretchr/testify v1.7.1-0.20210427113832-6241f9ab9942
	github.com/urfave/cli/v2 v2.3.0
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1
	google.golang.org/grpc v1.47.0
	google.golang.org/grpc/cmd/protoc-gen-go-grpc v1.2.0
	google.golang.org/protobuf v1.28.0
)

replace google.golang.org/grpc => github.com/grpc/grpc-go v1.41.0
