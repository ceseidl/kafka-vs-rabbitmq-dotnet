# Contributing / Contribuindo

[English](#english) | [Português](#português)

## English

Thanks for your interest in `kafka-vs-rabbitmq-dotnet`! This repository is **didactic code** that accompanies an article, so it is kept small and focused. A pull request that makes an example easier to understand, fixes a bug or improves the docs is very welcome. Large new features may be declined to keep the example readable. For anything bigger than a small fix, please **open an issue first**.

### How changes get into `main`

`main` is protected: every change goes through a **pull request**, the **`build` check** must be green, and the maintainer (Carlos Eduardo Seidl) reviews and merges it. Nobody pushes directly to `main`.

1. Fork the repository and create a branch from `main`.
2. Make your change and run the example locally (below).
3. Open a pull request to `main` and explain what and why.

For pull requests from forks, the maintainer has to approve the CI run before it starts, so it may take a little while.

### Run it locally

You need the [.NET 10 SDK](https://dotnet.microsoft.com/download).
- Docker is needed to run the brokers.

```bash
docker compose up -d --wait
dotnet build
dotnet run --no-build -- rabbit
dotnet run --no-build -- kafka
docker compose down -v
```

The CI (`.github/workflows/ci.yml`) runs the same example and **checks its output**. If your change alters the output, update the "expected output" in both READMEs and the checks in the workflow.

### Conventions

- **Two languages.** READMEs come in English (`README.md`) and Portuguese (`README.pt-BR.md`). Code comments use `// EN:` and `// PT:` lines, and text shown to the user is written as `English / Português`. If you only write one language, say so in the pull request and I will complete the other.
- **Do not rename identifiers.** Class, method, route and tool names (many in Portuguese) are referenced by the article.
- **Commit messages** follow the existing style: `feat:`, `fix:`, `docs:`, `ci:`.
- By contributing you agree that your work is licensed under the [MIT License](LICENSE).

Security problems go through the private channel in [SECURITY.md](SECURITY.md), not through issues.

## Português

Obrigado pelo interesse no `kafka-vs-rabbitmq-dotnet`! Este repositório é **código didático** que acompanha um artigo, então é mantido pequeno e focado. Um pull request que facilita o entendimento de um exemplo, corrige um bug ou melhora a documentação é muito bem-vindo. Funcionalidades grandes podem ser recusadas para manter o exemplo legível. Para qualquer coisa maior que uma correção pequena, **abra uma issue antes**.

### Como as mudanças chegam na `main`

A `main` é protegida: toda mudança passa por **pull request**, o check **`build`** precisa estar verde, e o mantenedor (Carlos Eduardo Seidl) revisa e faz o merge. Ninguém faz push direto na `main`.

1. Faça um fork do repositório e crie uma branch a partir da `main`.
2. Faça sua mudança e rode o exemplo localmente (abaixo).
3. Abra um pull request para a `main` explicando o quê e por quê.

Em pull requests de forks, o mantenedor precisa aprovar a execução do CI antes de ela começar, então pode demorar um pouco.

### Rodando localmente

Você precisa do [SDK do .NET 10](https://dotnet.microsoft.com/download).
- O Docker é necessário para rodar os brokers.

```bash
docker compose up -d --wait
dotnet build
dotnet run --no-build -- rabbit
dotnet run --no-build -- kafka
docker compose down -v
```

O CI (`.github/workflows/ci.yml`) roda o mesmo exemplo e **confere a saída**. Se a sua mudança altera a saída, atualize a "saída esperada" nos dois READMEs e as verificações do workflow.

### Convenções

- **Dois idiomas.** Os READMEs existem em inglês (`README.md`) e português (`README.pt-BR.md`). Comentários de código usam as linhas `// EN:` e `// PT:`, e textos exibidos ao usuário são escritos como `English / Português`. Se você escrever em um idioma só, avise no pull request e eu completo o outro.
- **Não renomeie identificadores.** Nomes de classes, métodos, rotas e tools (muitos em português) são citados no artigo.
- **Mensagens de commit** seguem o estilo existente: `feat:`, `fix:`, `docs:`, `ci:`.
- Ao contribuir, você concorda que o seu trabalho é licenciado sob a [Licença MIT](LICENSE).

Problemas de segurança vão pelo canal privado em [SECURITY.md](SECURITY.md), não por issues.
