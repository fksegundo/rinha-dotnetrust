# rinha-dotnetrust

Branch de submissao da solucao `rinha-dotnetrust` para a Rinha de Backend 2026.

Imagens publicas usadas por este compose:

- `filonsegundo/rinha-dotnetrust-api:submission`
- `filonsegundo/rinha-dotnetrust-lb:submission`

Suba com:

```bash
docker compose up -d
```

A aplicacao responde em:

- `GET /ready`
- `POST /fraud-score`

na porta `9999`.
