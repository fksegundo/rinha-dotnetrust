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

- score: `5867.50`
- p99: `1.10ms`
- false positives: `1`
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

- `filonsegundo/rinha-dotnetrust-api:submission`
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

- score: `5867.50`
- p99: `1.10ms`
- false positives: `1`
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

- `filonsegundo/rinha-dotnetrust-api:submission`
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
