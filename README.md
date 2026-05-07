# rinha-dotnetrust

Submissao da nossa solucao para a Rinha de Backend 2026.

Stack final:

- API em C# / .NET 10 NativeAOT
- busca nativa em Rust via P/Invoke
- indice `RNATIDX2` exato com particionamento, pruning por bounding box e scan AVX2 em blocos de 8 vetores
- load balancer proprio via imagem Docker separada
- pre-processamento offline do dataset oficial para gerar os indices dentro da imagem da API

Melhor resultado local validado no compose oficial:

- score: `5607.70`
- p99: `2.00ms`
- false positives: `1`
- false negatives: `0`
- http errors: `0`

## Estrutura

- `src/`: API e core da solucao
- `native/rinha-native/`: motor de busca nativo em Rust
- `tools/`: preprocessamento e verificador local
- `submission/`: Dockerfile da API e `docker-compose.yml` de submissao
- `bench/`: scripts de smoke e benchmark local

## Repositorio de submissao

Este repositorio e o formato "release":

- sem docs de laboratorio
- compose apontando para imagens
- Dockerfile da API mantido para reproducao e para publicar a imagem da aplicacao

O laboratorio continua separado em `../rinha-dotnet10`.

## Imagens

O compose de submissao usa estas variaveis:

- `APP_IMAGE` - default: `fksegundo/rinha-dotnetrust-api:submission`
- `LB_IMAGE` - default: `fksegundo/rinha-dotnetrust-lb:latest`

Voce pode sobrescrever com `.env` dentro de `submission/` ou exportando no shell.

Exemplo:

```bash
cd submission
cat > .env <<'EOF'
APP_IMAGE=fksegundo/rinha-dotnetrust-api:submission
LB_IMAGE=fksegundo/rinha-dotnetrust-lb:latest
EOF
docker compose up -d
```

## Publicar a imagem da API

O build da API precisa do dataset oficial em:

`resources/references.json.gz`

Esse arquivo nao entra no git. Coloque-o ali antes do build.

Build local:

```bash
docker build \
  -f submission/Dockerfile \
  -t fksegundo/rinha-dotnetrust-api:submission \
  .
```

Push:

```bash
APP_IMAGE=fksegundo/rinha-dotnetrust-api:submission \
./scripts/publish-api-image.sh
```

## Rodar localmente

Com as imagens publicadas:

```bash
cd submission
docker compose up -d
curl http://localhost:9999/ready
```

Para benchmark local, este workspace ainda assume a presenca do repositorio oficial ao lado:

```bash
./bench/run-smoke.sh
./bench/run-official-local.sh
```

## Observacoes

- o `docker-compose.yml` de submissao usa apenas imagens
- o codigo do load balancer fica em um repositorio separado
- o build Rust da busca nativa usa `target-cpu=haswell` por default na imagem da API
