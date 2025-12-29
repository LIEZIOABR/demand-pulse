\# 📘 Manual de Operação - DEMAND PULSE



Este documento contém as instruções para gerenciar e manter o sistema de inteligência turística \*\*ABR ALL-IN-ONE\*\*.



\## 🚀 Como o sistema funciona

O sistema é 100% automatizado e funciona em três camadas:

1\. \*\*Motor (Python)\*\*: O arquivo `update\_pulse\_v2.py` busca dados no Google Trends.

2\. \*\*Robô (GitHub Actions)\*\*: Executa o motor automaticamente todo dia à meia-noite.

3\. \*\*Painel (HTML)\*\*: Exibe os dados reais para o usuário final no Netlify.



\## 🛠 Como atualizar os dados manualmente

Se você não quiser esperar até a meia-noite:

1\. No GitHub, vá na aba \*\*Actions\*\*.

2\. Clique em \*\*"Atualizar Dados do Google Trends"\*\* à esquerda.

3\. Clique no botão \*\*"Run workflow"\*\* à direita e confirme.

4\. Quando a bolinha ficar verde, o site estará atualizado.



\## 📍 Como alterar ou adicionar destinos

Para mudar as cidades monitoradas, você deve editar o arquivo `update\_pulse\_v2.py`:

1\. Clique no arquivo e no ícone do \*\*lápis\*\*.

2\. Procure a lista `DESTINATIONS`.

3\. Altere o `id` (nome interno) e o `kw` (termo de busca no Google).

&nbsp;  \*Exemplo:\* `{"id": "novo\_destino", "kw": "Hoteis em Gramado"}`

4\. Salve as alterações (\*\*Commit changes\*\*).



\## ⚠️ Resolução de Problemas

\* \*\*Bolinha Vermelha no Actions\*\*: Geralmente é um bloqueio temporário do Google por excesso de acessos. Espere 30 minutos e tente de novo. O site NÃO estraga quando isso acontece; ele mantém os últimos dados válidos.

\* \*\*Cards não aparecem no site\*\*: Verifique se o arquivo `pulse-data.json` existe e se não está vazio. Se estiver, rode o workflow manualmente.

\* \*\*Logo sumiu\*\*: Verifique se o arquivo `logo.png` está na pasta principal do GitHub.



\## 🔐 Próximos Passos Sugeridos

\* Configurar o \*\*Netlify Identity\*\* para controle de acesso com login e senha.

\* Apontar o domínio profissional na \*\*UOL Host\*\* para o link do Netlify.



---

\*Desenvolvido para ABR ALL-IN-ONE - 2025\*



