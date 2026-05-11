# rinha-dotnetrust

## PT-BR

Repositorio principal da solucao `rinha-dotnetrust` para a Rinha de Backend 2026.

Stack:

- API em C# / .NET 10 NativeAOT
- busca nativa em Rust via P/Invoke
- indice `RNATIDX2` exato com pruning por bounding box e scan AVX2 em blocos de 8 vetores
- load balancer proprio em repositorio separado
- pre-processamento offline do dataset oficial para gerar os indices dentro da imagem

Melhor validacao local usando as imagens publicadas:

- score: `5959.38`
- p99: `1.10ms`
- false positives: `0`
- false negatives: `0`
- http errors: `0`

Estrutura:

- `src/`: API e core
- `native/rinha-native/`: motor nativo em Rust
- `tools/`: preprocessamento e verificacao
- `submission/`: Dockerfile da API e compose de validacao local
- `bench/`: scripts locais
- `Makefile`: atalhos para build, publish e benchmark

Imagens publicadas:

- `filonsegundo/rinha-dotnetrust-api:v0.3`
- `filonsegundo/rinha-dotnetrust-lb:submission`

## EN

Main repository for the `rinha-dotnetrust` solution targeting Rinha de Backend 2026.

Stack:

- C# / .NET 10 NativeAOT API
- native Rust search via P/Invoke
- exact `RNATIDX2` index with bounding-box pruning and AVX2 8-lane block scans
- standalone load balancer hosted in a separate repository
- offline preprocessing of the official dataset to generate the runtime indexes

Best local validation using the published images:

- score: `5959.38`
- p99: `1.10ms`
- false positives: `0`
- false negatives: `0`
- http errors: `0`

Layout:

- `src/`: API and core logic
- `native/rinha-native/`: native Rust engine
- `tools/`: preprocessing and verification tools
- `submission/`: API Dockerfile and local validation compose
- `bench/`: local scripts
- `Makefile`: shortcuts for build, publish and benchmark flows

Published images:

- `filonsegundo/rinha-dotnetrust-api:v0.3`
- `filonsegundo/rinha-dotnetrust-lb:submission`

## Quick Start

Build the solution:

```bash
make build
```

Publish the API image:

```bash
make publish-api
```

Start the published stack locally:

```bash
make compose-up
curl http://localhost:9999/ready
```

Run smoke and full benchmark:

```bash
make smoke
make bench
```

Benchmark notes:

- `make smoke` and `make bench` now use the published images and run k6 in Docker.
- `make smoke-build` and `make bench-build` keep the old local rebuild flow.
- `make bench-diagnostic` adds the diagnostic compose overlay so you can vary CPU split, probes, and cpuset without editing the submission compose.
- `make bench-matrix` runs the versioned scenario table in `bench/diagnostic-matrix.tsv` and writes a `summary.tsv` plus one artifact directory per scenario.
- Benchmark artifacts and runtime evidence are written under `artifacts/`.
