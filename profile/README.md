**LDC - SBS OMCS | Guia do Usuário - Repositório GitHub + Databricks**

Esta organização foi criada para organizar e versionar os notebooks de dados desenvolvidos pelas equipes de Originação, MDM, CCC, Suporte e Sustentabilidade do SBS na LDC. Abaixo estão as instruções e boas práticas para o uso adequado desse ambiente integrado com GitHub e Databricks.

────────────────────────────────────────────

📁 CRIAÇÃO DE NOTEBOOKS NO DATABRICKS

────────────────────────────────────────────
- Todos os notebooks devem ser criados dentro da pasta do repositório clonado no seu Workspace do Databricks.
- Criar notebooks sempre dentro de uma branch específica (ex: feat/nome-do-pipeline).
- Sempre criar uma branch `feat/` para um pipeline de dados dentro da branch `dev`
- Nomear notebooks de forma padronizada:nome-do-pipeline.ipynb

────────────────────────────────────────────

🌿 FLUXO DE TRABALHO (CI/CD Simplificado)

────────────────────────────────────────────
1. Criar branch `feat/nome-do-pipeline` a partir da `dev`.
2. Desenvolver o notebook no Databricks dentro do repositório Git clonado.
3. Commitar as alterações na branch `feat/...` com boas práticas de mensagens.
4. Realizar pull request no GitHub para a branch `main` (merge controlado).
5. O Databricks Workflows roda os jobs diretamente da branch `main` do GitHub.

────────────────────────────────────────────

📝 NOMENCLATURA DE COMMITS

────────────────────────────────────────────

Prefixos a serem usados nas mensagens de commit:
- feat: Nova funcionalidade (ex: feat: add novo pipeline produtores ativos)
- fix: Correção de bugs
- chore: Tarefas operacionais ou de manutenção (excluir arquivos, renomear, mover)
- docs: Alterações de documentação (ex: README, guias, Docscript dos notebooks)
- refactor: Melhorias no código sem alterar funcionalidades
- test: Adição de testes

────────────────────────────────────────────

🧾 BOAS PRÁTICAS PARA DESCRIÇÃO DE COMMITS

────────────────────────────────────────────
- A primeira linha deve ser curta e objetiva (máx. 72 caracteres).
- Abaixo, se necessário, adicione uma descrição explicando a lógica ou mudanças.
Exemplo:
feat: add ETL produtores ativos
Adicionado pipeline que gera relatório dos fornecedores ativos no AS400.

────────────────────────────────────────────

🔄 INTEGRAÇÃO COM DATABRICKS WORKFLOWS

────────────────────────────────────────────
- Os Jobs do Databricks devem sempre estar conectados ao repositório GitHub na branch `main`.
- A origem do job deve ser configurada com:
  Source: Git provider
  Path: Caminho relativo do notebook no repositório
- Nome dos Jobs devem seguir o padrão:
  - ETL_nome-do-pipeline (para geração de dados/parquets)
  - WF_nome-do-pipeline (para outros tipos)
- Descrição dos Jobs devem ser em inglês.
- Sempre adicionar Tags nos Jobs com:
  SBS-OMCS: Origination; MDM; Support; CCC

────────────────────────────────────────────

👥 EQUIPES

────────────────────────────────────────────
- > Originação: [`ldc-sbs-origination`](https://github.com/ldc-sbs-omcs/origination-data-pipelines)
- > MDM: [`ldc-sbs-MDM`](https://github.com/ldc-sbs-omcs/MDM-data-pipelines)
- > Suporte: [`ldc-sbs-suporte`](https://github.com/ldc-sbs-omcs/suporte-data-pipelines)
- > CCC: [`ldc-sbs-CCC`](https://github.com/ldc-sbs-omcs/CCC-data-pipelines)
- > Organização: [`ldc-sbs-omcs`](https://github.com/ldc-sbs-omcs)

────────────────────────────────────────────

📚 DOCUMENTAÇÃO E TREINAMENTO

────────────────────────────────────────────

Está sendo preparado um material de apoio para o uso do GitHub e Databricks nesse fluxo. Em breve será compartilhado com todos os times da OMCS.

────────────────────────────────────────────

✔️ CONVENÇÕES FINAIS
────────────────────────────────────────────
- **Nunca realize merge direto da branch `dev` para `main`.**
- Cada novo notebook/pipeline deve ser criado em uma branch `feat/`
- Não inclua dados sensíveis ou credenciais nos notebooks

────────────────────────────────────────────

**Dúvidas ou sugestões, entre em contato com o responsável pelo repositório Everaldo Santos.**
