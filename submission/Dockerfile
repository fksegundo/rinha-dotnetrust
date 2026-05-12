# ─── Stage 1: Build Native Rust Library ──────────────────────────────────────
FROM rust:1.95-bookworm AS native-build

ARG RUST_TARGET_CPU=haswell
ENV RUSTFLAGS="-C target-cpu=${RUST_TARGET_CPU}"

WORKDIR /native

# Cache-friendly copy of cargo files first
COPY native/rinha-native/Cargo.toml native/rinha-native/Cargo.lock* ./
COPY native/rinha-native/src ./src

# High-speed Cargo build with cache mounts
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/native/target \
    cargo build --release && \
    cp /native/target/release/librinha_native.so /librinha_native.so

# ─── Stage 2: Build .NET API and Preprocess Index ────────────────────────────
FROM mcr.microsoft.com/dotnet/sdk:10.0-noble AS build

WORKDIR /src

ARG RINHA_NATIVE_LEAF_SIZE=192

RUN apt-get update \
    && apt-get install -y --no-install-recommends clang zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

# Optimize layer caching: restore packages first before copying all source code
COPY Rinha.slnx ./
COPY src/Rinha.Api/Rinha.Api.csproj src/Rinha.Api/
COPY src/Rinha.Core/Rinha.Core.csproj src/Rinha.Core/
COPY tools/Rinha.Preprocess/Rinha.Preprocess.csproj tools/Rinha.Preprocess/

RUN --mount=type=cache,target=/root/.nuget/packages \
    dotnet restore src/Rinha.Api/Rinha.Api.csproj -r linux-x64

RUN --mount=type=cache,target=/root/.nuget/packages \
    dotnet restore tools/Rinha.Preprocess/Rinha.Preprocess.csproj

# Copy remaining code files
COPY . ./

RUN test -f /src/resources/references.json.gz

# Publish NativeAOT API with maximum execution speed optimizations
RUN --mount=type=cache,target=/root/.nuget/packages \
    dotnet publish src/Rinha.Api/Rinha.Api.csproj \
    -c Release \
    -r linux-x64 \
    -o /out \
    --self-contained true \
    /p:PublishAot=true \
    /p:OptimizationPreference=Speed \
    /p:StripSymbols=true

# Copy the native compiled binary to the published output
COPY --from=native-build /librinha_native.so /out/

# Run the preprocessor to generate the KD-Tree native index
RUN --mount=type=cache,target=/root/.nuget/packages \
    dotnet run --project tools/Rinha.Preprocess/Rinha.Preprocess.csproj \
    -c Release -- \
    /src/resources/references.json.gz \
    /out/index/native.idx \
    native ${RINHA_NATIVE_LEAF_SIZE}

# ─── Stage 3: High-Performance Runtime Image ────────────────────────────────
FROM mcr.microsoft.com/dotnet/runtime-deps:10.0-noble-chiseled-extra

WORKDIR /app

COPY --from=build /out/ ./

ENTRYPOINT ["/app/Rinha.Api"]
