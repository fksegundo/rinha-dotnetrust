# rinha-dotnetrust

## PT-BR

Branch de submissao da solucao `rinha-dotnetrust` para a Rinha de Backend 2026.

Imagens publicas usadas por este compose:

- `filonsegundo/rinha-dotnetrust-api:v0.3`
- `filonsegundo/rinha-dotnetrust-lb:submission`

Validacao local da tag `v0.3`:

- score: `5969.42`
- p99: `1.07ms`
- false positives: `0`
- false negatives: `0`
- http errors: `0`

Suba com:

```bash
docker compose up -d
```

A aplicacao responde em:

- `GET /ready`
- `POST /fraud-score`

na porta `9999`.

## EN

Submission branch for the `rinha-dotnetrust` solution targeting Rinha de Backend 2026.

Public images used by this compose file:

- `filonsegundo/rinha-dotnetrust-api:v0.3`
- `filonsegundo/rinha-dotnetrust-lb:submission`

Local validation for the `v0.3` tag:

- score: `5969.42`
- p99: `1.07ms`
- false positives: `0`
- false negatives: `0`
- http errors: `0`

Start it with:

```bash
docker compose up -d
```

The application exposes:

- `GET /ready`
- `POST /fraud-score`

on port `9999`.
