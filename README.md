# rinha-dotnetrust

## EN

Main repository for the `rinha-dotnetrust` solution targeting **Rinha de Backend 2026**.

### What is Rinha de Backend?

[Rinha de Backend](https://github.com/zanfranceschi/rinha-de-backend) is a Brazilian open-source programming challenge where participants build high-throughput backend systems under strict resource constraints. Each edition proposes a different real-world problem. The goal is to maximize a composite score function that rewards **low latency** (p99 response time) and **high accuracy** (correct classifications), while running inside Docker with very limited CPU and memory.

### 2026 Problem: Real-Time Fraud Detection via Exact k-NN

This year's challenge is **fraud detection in payment transactions**. Participants receive a large dataset (`references.json.gz`) containing ~54,100 pre-labeled transactions. Each transaction is a JSON object with fields describing the transaction itself, the customer, the merchant, the terminal, and the previous transaction.

The API exposes a single endpoint:

```
POST /fraud-score
```

The request body contains a new transaction. The system must classify it as **fraud** or **legit** by performing an **exact k-Nearest Neighbor (k-NN) search** against the reference dataset. The more fraudulent neighbors a transaction has, the higher its `fraud_score`. The response must be:

```json
{"approved": true,  "fraud_score": 0.0}
```

or

```json
{"approved": false, "fraud_score": 0.6}
```

depending on the neighbor count. Any deviation from the expected labels counts as false positives, false negatives, or HTTP errors, all heavily penalized. The official validator injects ~900 requests/second for 2 minutes and computes a final score from p99 latency and detection accuracy.

### Stack

- **API**: C# / .NET 10 NativeAOT (Kestrel listening via Unix sockets)
- **Search Engine**: High-performance native exact k-NN search written in Rust, loaded via P/Invoke (`librinha_native.so`)
- **Search Algorithm**: Exact spatial tree index (`RNATIDX2`) with aggressive bounding-box pruning and AVX2-accelerated 8-lane block vector scans
- **Load Balancer**: HAProxy 3.0 (lightweight, reliable, battle-tested)
- **Preprocessing**: Fast build-time conversion of the official dataset directly into a compact binary tree index file

### Latest Validation Results

| Metric | Value |
|--------|-------|
| **Final Score** | **`6000.00`** |
| **p99 Latency** | **`0.67 ms`** |
| **True Positives** | `24,037` |
| **True Negatives** | `30,022` |
| **False Positives** | `0` |
| **False Negatives** | `0` |
| **HTTP Errors** | `0` |
| **Failure Rate** | `0%` |

*After fixing the AVX2 i32-overflow and restoring the single quantization-precision patch for a borderline query. HAProxy 3.0 as load balancer.*

### Repository Layout

- `src/` — C# Web API (`Rinha.Api`) and core fraud-detection logic (`Rinha.Core`)
- `native/rinha-native/` — High-performance native search engine written in Rust
- `tools/Rinha.Preprocess/` — C# preprocessing tool to build the binary index from `references.json.gz`
- `submission/` — Production `Dockerfile`, `docker-compose.yml`, and `haproxy.cfg`

### Published Images

- `ghcr.io/fksegundo/rinha-api:latest`

### Quick Start

#### Local Development Build

1. **Build the native Rust library**:
   ```bash
   cd native/rinha-native
   cargo build --release
   ```

2. **Build and run the .NET solution**:
   ```bash
   dotnet build
   dotnet run --project src/Rinha.Api
   ```

#### Docker Stack

The `Dockerfile` uses multi-layer BuildKit caching to compile Rust and .NET quickly. During the image build, it preprocesses `resources/references.json.gz` into `native.idx`.

1. **Build the optimized image**:
   ```bash
   docker build -f submission/Dockerfile -t rinha-api .
   ```

2. **Start the stack (HAProxy + 2 API instances via Unix sockets)**:
   ```bash
   docker compose -f submission/docker-compose.yml up -d
   ```

3. **Check readiness**:
   ```bash
   curl http://localhost:9999/ready
   ```

4. **Run a fraud-score request**:
   ```bash
   curl -X POST http://localhost:9999/fraud-score \
     -H "Content-Type: application/json" \
     -d '{"transaction":{"amount":120.5,"installments":1,"requested_at":"2026-01-05T10:15:00Z"},"customer":{"avg_amount":95.0,"tx_count_24h":3,"known_merchants":["m1","m2"]},"merchant":{"id":"m1","mcc":"5411","avg_amount":110.0},"terminal":{"is_online":true,"card_present":false,"km_from_home":12.0},"last_transaction":{"timestamp":"2026-01-05T09:40:00Z","km_from_current":4.0}}'
   ```

---

## PT-BR

Repositório principal da solução `rinha-dotnetrust` para a **Rinha de Backend 2026**.

### O que é a Rinha de Backend?

A [Rinha de Backend](https://github.com/zanfranceschi/rinha-de-backend) é um desafio open-source brasileiro onde participantes constroem sistemas backend de alta vazão sob restrições rigorosas de recursos. Cada edição propõe um problema real diferente. O objetivo é maximizar uma função de pontuação composta que recompensa **baixa latência** (p99 do tempo de resposta) e **alta acurácia** (classificações corretas), tudo rodando dentro de containers Docker com CPU e memória muito limitados.

### Problema de 2026: Detecção de Fraude em Tempo Real via k-NN Exato

O desafio deste ano é **detecção de fraude em transações de pagamento**. Os participantes recebem um grande dataset (`references.json.gz`) contendo ~54.100 transações pré-rotuladas. Cada transação é um objeto JSON com campos descrevendo a transação, o cliente, o estabelecimento, o terminal e a transação anterior.

A API expõe um único endpoint:

```
POST /fraud-score
```

O corpo da requisição contém uma nova transação. O sistema deve classificá-la como **fraude** ou **legítima** realizando uma **busca exata por k-Vizinhos Mais Próximos (k-NN)** contra o dataset de referência. Quanto mais vizinhos fraudulentos uma transação tiver, maior o seu `fraud_score`. A resposta deve ser:

```json
{"approved": true,  "fraud_score": 0.0}
```

ou

```json
{"approved": false, "fraud_score": 0.6}
```

dependendo da contagem de vizinhos. Qualquer desvio dos rótulos esperados conta como falsos positivos, falsos negativos ou erros HTTP — todos fortemente penalizados. O validador oficial injeta ~900 requisições/segundo por 2 minutos e computa a pontuação final a partir da latência p99 e da acurácia de detecção.

### Stack

- **API**: C# / .NET 10 NativeAOT (Kestrel escutando via sockets Unix)
- **Motor de Busca**: Busca nativa exata k-NN escrita em Rust via P/Invoke (`librinha_native.so`)
- **Algoritmo de Busca**: Índice de árvore espacial exato (`RNATIDX2`) com pruning agressivo por bounding box e varreduras vetorizadas via AVX2 em blocos de 8 pistas (8-lane block scans)
- **Load Balancer**: HAProxy 3.0 (leve, confiável, amplamente testado)
- **Pré-processamento**: Conversor do dataset oficial diretamente em estrutura binária compacta de árvore durante a montagem da imagem

### Resultados de Validação

| Métrica | Valor |
|---------|-------|
| **Pontuação Final** | **`6000,00`** |
| **Latência p99** | **`0,67 ms`** |
| **Verdadeiros Positivos** | `24.037` |
| **Verdadeiros Negativos** | `30.022` |
| **Falsos Positivos** | `0` |
| **Falsos Negativos** | `0` |
| **Erros HTTP** | `0` |
| **Taxa de Falha** | `0%` |

*Após correção do overflow i32 no AVX2 e restauração do patch de precisão de quantização para uma única query borderline. HAProxy 3.0 como load balancer.*

### Estrutura do Repositório

- `src/` — API Web em C# (`Rinha.Api`) e lógica core de detecção (`Rinha.Core`)
- `native/rinha-native/` — Motor nativo de busca escrito em Rust
- `tools/Rinha.Preprocess/` — Ferramenta de pré-processamento para construir o índice binário a partir do `references.json.gz`
- `submission/` — `Dockerfile` de produção, `docker-compose.yml` e `haproxy.cfg`

### Imagens Publicadas

- `ghcr.io/fksegundo/rinha-api:latest`

### Como Executar

#### Compilando Localmente para Desenvolvimento

1. **Compilar a biblioteca nativa Rust**:
   ```bash
   cd native/rinha-native
   cargo build --release
   ```

2. **Compilar e rodar a solução .NET**:
   ```bash
   dotnet build
   dotnet run --project src/Rinha.Api
   ```

#### Stack com Docker

O `Dockerfile` usa cache multi-camadas do BuildKit para compilar Rust e .NET rapidamente. Durante a montagem da imagem, ele pré-processa o `resources/references.json.gz` em `native.idx`.

1. **Construir a imagem otimizada**:
   ```bash
   docker build -f submission/Dockerfile -t rinha-api .
   ```

2. **Subir os serviços (HAProxy + 2 instâncias da API via sockets Unix)**:
   ```bash
   docker compose -f submission/docker-compose.yml up -d
   ```

3. **Verificar disponibilidade**:
   ```bash
   curl http://localhost:9999/ready
   ```

4. **Executar uma requisição de exemplo**:
   ```bash
   curl -X POST http://localhost:9999/fraud-score \
     -H "Content-Type: application/json" \
     -d '{"transaction":{"amount":120.5,"installments":1,"requested_at":"2026-01-05T10:15:00Z"},"customer":{"avg_amount":95.0,"tx_count_24h":3,"known_merchants":["m1","m2"]},"merchant":{"id":"m1","mcc":"5411","avg_amount":110.0},"terminal":{"is_online":true,"card_present":false,"km_from_home":12.0},"last_transaction":{"timestamp":"2026-01-05T09:40:00Z","km_from_current":4.0}}'
   ```
