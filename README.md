# rinha-dotnetrust

## PT-BR

Repositório principal da solução `rinha-dotnetrust` para a Rinha de Backend 2026.

Esta versão está extremamente limpa e reestruturada, contendo **apenas o caminho feliz de máxima performance**, sem restos de códigos e scripts experimentais de desenvolvimento.

### Stack:

- **API**: C# / .NET 10 NativeAOT (Kestrel escutando via sockets Unix)
- **Motor de Busca**: Busca nativa exata escrita em Rust via P/Invoke (`librinha_native.so`)
- **Algoritmo de Busca**: Índice de árvore espacial exato (`RNATIDX2`) com pruning agressivo por bounding box e varreduras vetorizadas via AVX2 em blocos de 8 pistas (8-lane block scans)
- **Load Balancer**: Load Balancer próprio e leve de alta vazão (hospedado em repositório separado)
- **Pré-processamento**: Conversor do dataset oficial diretamente em estrutura binária compacta de árvore durante a montagem do container

### Validação Local Recente:

- **Score**: `5959.38`
- **p99**: `1.10ms`
- **False Positives**: `0`
- **False Negatives**: `0`
- **Erros HTTP**: `0`

### Estrutura do Repositório:

- `src/`: Lógica da API Web e Core.
- `native/rinha-native/`: Motor nativo em Rust que implementa a busca kNN vetorial exata.
- `tools/Rinha.Preprocess/`: Pré-processador escrito em C# para indexar o dataset oficial `references.json.gz`.
- `submission/`: `Dockerfile` de produção altamente otimizado e `docker-compose.yml` para validação e testes locais.

### Imagens Publicadas:

- `filonsegundo/rinha-dotnetrust-api:v0.4.1`
- `filonsegundo/rinha-dotnetrust-lb:submission`

---

## EN

Main repository for the `rinha-dotnetrust` solution targeting Rinha de Backend 2026.

This version is extremely clean and restructured, retaining **only the high-performance happy path** and removing all experimental legacy code, benchmarks, and scripts.

### Stack:

- **API**: C# / .NET 10 NativeAOT (Kestrel listening via Unix sockets)
- **Search Engine**: High-performance native exact search written in Rust, loaded via P/Invoke (`librinha_native.so`)
- **Search Algorithm**: Exact spatial tree index (`RNATIDX2`) with aggressive bounding-box pruning and AVX2-accelerated 8-lane block vector scans
- **Load Balancer**: Custom, lightweight, high-throughput load balancer (hosted in a separate repository)
- **Preprocessing**: Fast compilation-time conversion of the official dataset directly into a compact binary tree index file

### Layout:

- `src/`: C# Web API and core logic.
- `native/rinha-native/`: High-performance native search engine written in Rust.
- `tools/Rinha.Preprocess/`: C# preprocessing tool to index `references.json.gz`.
- `submission/`: Optimized production `Dockerfile` and local testing `docker-compose.yml`.

### Published Images:

- `filonsegundo/rinha-dotnetrust-api:v0.4.1`
- `filonsegundo/rinha-dotnetrust-lb:submission`

---

## Quick Start / Como Executar

### 🛠️ Compilando Localmente para Desenvolvimento

1. **Compilar a biblioteca nativa Rust**:
   ```bash
   cd native/rinha-native
   cargo build --release
   ```

2. **Compilar e rodar a solução .NET**:
   ```bash
   dotnet build
   ```

### 🐳 Construindo e Executando a Stack com Docker

O `Dockerfile` é totalmente otimizado com cache multi-camadas (BuildKit) para compilar o Rust e o .NET rapidamente. No processo de montagem, ele executa o pré-processamento do dataset `resources/references.json.gz` e gera o índice `native.idx`.

1. **Construir a imagem local otimizada**:
   ```bash
   docker build -f submission/Dockerfile -t filonsegundo/rinha-dotnetrust-api:v0.4.1 .
   ```

2. **Subir os serviços (Load Balancer + 2 instâncias da API)**:
   ```bash
   docker compose -f submission/docker-compose.yml up -d
   ```

3. **Verificar a disponibilidade das APIs**:
   ```bash
   curl http://localhost:9999/ready
   ```
