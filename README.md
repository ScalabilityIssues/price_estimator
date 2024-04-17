# price_estimator

# gRPC configuration
```
python -m grpc_tools.protoc -Iproto/ --python_out=src/protos --pyi_out=src/protos --grpc_python_out=src/protos proto/priceest/prices.proto proto\commons\field_behavior.proto
```
