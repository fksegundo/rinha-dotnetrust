FROM rust:1.95-bookworm AS native-build

ARG RUST_TARGET_CPU=haswell
ENV RUSTFLAGS="-C target-cpu=${RUST_TARGET_CPU}"

WORKDIR /native
COPY native/rinha-native/ ./
RUN cargo build --release

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build

WORKDIR /src
ARG RINHA_NATIVE_LEAF_SIZE=192
RUN apt-get update \
    && apt-get install -y --no-install-recommends clang zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

COPY . ./

RUN test -f /src/resources/references.json.gz

RUN dotnet publish src/Rinha.Api/Rinha.Api.csproj \
    -c Release \
    -r linux-x64 \
    -o /out \
    /p:PublishAot=true \
    --self-contained true

RUN dotnet run --project tools/Rinha.Preprocess/Rinha.Preprocess.csproj \
    -c Release -- \
    /src/resources/references.json.gz \
    /out/index/native.idx \
    native ${RINHA_NATIVE_LEAF_SIZE}

COPY --from=native-build /native/target/release/librinha_native.so /out/

FROM mcr.microsoft.com/dotnet/runtime-deps:10.0-noble-chiseled-extra

WORKDIR /app
COPY --from=build /out/ ./

ENTRYPOINT ["/app/Rinha.Api"]
