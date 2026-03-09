6 de mar. de 2026

## \[Pismo\] Sync Poc Monday

Convidados [Erico Silva](mailto:erico.silva@databricks.com) [Gui Sepe](mailto:gui.sepe@databricks.com) [Jose Carlos Esteca Junior](mailto:junior.esteca@databricks.com)

Anexos [\[Pismo\] Sync Poc Monday](https://www.google.com/calendar/event?eid=NDV2cTN1MTBldm9jOWsxMTRocDcxbjRpNDUganVuaW9yLmVzdGVjYUBkYXRhYnJpY2tzLmNvbQ) 

Registros da reunião [Transcrição](?tab=t.n0f9t1fm0hzb) 

### Resumo

Revisão da POC do conector Monday.com, aproveitando a documentação de um hackathon interno para testar código existente e discutir a importância do Unity Catalog na Visa, focando na otimização de workloads para reduzir custos.

**POC do Conector Monday.com**  
O cliente aceitou a Prova de Conceito para o conector Monday.com e fornecerá 1 engenheiro, com a equipe coordenada para definir critérios de sucesso. Gui Sepe iniciou a coordenação com Artur para determinar o início da POC.

**Descobertas Técnicas para POC**  
Foi encontrada documentação de um hackathon interno, incluindo um conector Monday.com com esquemas de boards e items, o que será usado como ponto de partida. O plano é testar este código existente em um ambiente E2 antes de construir algo do zero.

**Otimização de Workloads na Visa**  
A equipe discutiu a necessidade do Unity Catalog na Visa e a principal reclamação de workloads. A recomendação é migrar o pipeline para um único workflow do Databricks para consolidar as tarefas e reduzir o tempo de setup da máquina.

### Detalhes

* **POC do Conector de Comunidade Monday.com**: A equipe discutiu a Prova de Conceito (POC) para um conector Monday.com, que o cliente aceitou. O cliente concordou em fornecer um engenheiro para a POC quando a equipe estiver tecnicamente pronta, e a falta de suporte para conectores de comunidade não é um obstáculo, visto que o cliente nunca abriu um chamado de suporte antes ([00:00:00](?tab=t.n0f9t1fm0hzb#heading=h.rfwd0xkn7gu1)). Gui Sepe iniciou um chat com o Artur para coordenar o início da POC e determinar os critérios de sucesso, envolvendo Jose Carlos Esteca Junior e Erico Silva ([00:14:14](?tab=t.n0f9t1fm0hzb#heading=h.kruoacec8hf7)).

* **Descobertas Técnicas para a POC**: Jose Carlos Esteca Junior compartilhou que encontrou recursos de um \*hackathon\* interno que incluem documentação para conectores Monday, Zendesk e Stripe, especificamente o Monday ([00:01:21](?tab=t.n0f9t1fm0hzb#heading=h.rn7qo9g1gj2m)). Esta documentação inclui como obter a API e como o conjunto de dados é estruturado, cobrindo "boards e items" que podem ser relevantes para o que Artur deseja ([00:02:31](?tab=t.n0f9t1fm0hzb#heading=h.njjc1mtalkk9)). A equipe planeja testar esse código existente para o Monday.com como ponto de partida antes de considerar construir algo do zero ([00:04:49](?tab=t.n0f9t1fm0hzb#heading=h.vhd523qtr0b9)).

* **Análise da Documentação Técnica do Conector Monday.com**: O material encontrado inclui exemplos de configuração para o pipeline e menções a testes unitários e de integração para verificar o funcionamento do conector ([00:03:17](?tab=t.n0f9t1fm0hzb#heading=h.wkgapn3p5kml)). Foi observada a necessidade de estudar a documentação para entender a geração e o uso do \*token\* da API para chamadas e para estruturar os \*datasets\*. A documentação detalha as funções implementadas para obter o esquema de dados (\`get schema\`) e acessar \*boards\* ([00:03:57](?tab=t.n0f9t1fm0hzb#heading=h.yxbvhfvqhil5)).

* **Próximos Passos para a Implementação Técnica**: Erico Silva e Jose Carlos Esteca Junior planejam configurar um ambiente para testar o código existente do conunto de conectores, o que pode ser feito em um ambiente E2 ([00:04:49](?tab=t.n0f9t1fm0hzb#heading=h.vhd523qtr0b9)) ([00:08:11](?tab=t.n0f9t1fm0hzb#heading=h.3685x2cqljd0)). Jose Carlos Esteca Junior mencionou que há \*templates\* disponíveis para desenvolver novos componentes, mas eles concordaram em priorizar o teste do material já encontrado ([00:09:07](?tab=t.n0f9t1fm0hzb#heading=h.7ojgj69kc6bt)). Erico Silva deverá trabalhar em conjunto com Jose Carlos Esteca Junior na POC do conector Monday.com ([00:13:03](?tab=t.n0f9t1fm0hzb#heading=h.66p1h3samgb5)).

* **Discussão sobre o Unity Catalog na Visa**: A equipe abordou a importância do Unity Catalog após uma conversa de Erico Silva com Éder, que expressou a necessidade do Unity para aplicar seus estudos de Databricks ([00:16:15](?tab=t.n0f9t1fm0hzb#heading=h.29dmrjrlo31e)). Há uma suspeita de que a Visa utilize o Unity, especialmente em ambientes de POC ou se houver requisitos mínimos de governança, mas isso ainda precisa ser confirmado ([00:17:16](?tab=t.n0f9t1fm0hzb#heading=h.ny7plk123tns)).

* **Otimização de Workloads na Visa**: Erico Silva relatou que a principal reclamação na Visa é que eles abrem um \*job\* para cada etapa do \*pipeline\*, o que resulta em custos e tempos de inicialização excessivos, demorando 8 minutos para subir o ambiente e mais de uma hora para processar um volume pequeno. A recomendação é migrar o \*pipeline\* para um único \*workflow\* do Databricks para consolidar as tarefas e reduzir o tempo de \*setup\* da máquina ([00:18:39](?tab=t.n0f9t1fm0hzb#heading=h.pa561d8adx0e)).

* **Acompanhamento das Recomendações de Arquitetura (Pismo)**: O Rômulo sugeriu que a equipe fale com o Thiago apenas em abril, mas Erico Silva acredita que eles já têm dados suficientes para criar uma primeira rodada de recomendações. Erico Silva analisará os insumos para gerar um documento ou ver se é necessário coletar mais informações ([00:20:51](?tab=t.n0f9t1fm0hzb#heading=h.zh1dzty99044)).

* **Análise das Respostas do LNA (Pismo)**: O painel do \*survey\* do LNA (Avaliação de Necessidades de Aprendizado) mostrou apenas uma resposta para o ano atual, de Pedro Correa, com outras respostas sendo do ano anterior ([00:22:07](?tab=t.n0f9t1fm0hzb#heading=h.133l4emnc5be)). Gui Sepe instruiu Erico Silva a fazer um acompanhamento, incluindo todos em um e-mail de \*status\* para Rômulo, cobrando mais respostas e indicando os próximos passos de análise e futuras recomendações ([00:24:47](?tab=t.n0f9t1fm0hzb#heading=h.moqjv3lly7jf)).

* **Discussões de Arquitetura com o Ridec**: A equipe precisa fazer um \*follow-up\* com o Ridec para agendar uma sessão de visão geral da arquitetura, um compromisso assumido anteriormente. Jose Carlos Esteca Junior e Erico Silva devem dar prosseguimento a essa conversa, pois Jose Carlos Esteca Junior já havia apresentado a arquitetura rapidamente ([00:25:50](?tab=t.n0f9t1fm0hzb#heading=h.nmu4vy557mgb)).

### Próximas etapas sugeridas

- [ ] Erico Silva e Jose Carlos Esteca Junior testarão o community connector de Monday que Jose Carlos Esteca Junior encontrou, subindo o ambiente para ver se funciona.  
- [ ] Erico Silva falará com Pedro San Lourense para entender como foi a história da POC de SDP com o Ridec e puxar essa linha de investigação.  
- [ ] Erico Silva fará uma primeira rodada de análise das informações coletadas para gerar um documento de recomendação e pingará a equipe para verificar o status de preenchimento do LNA (Survei) para cobrar o Rômulo.  
- [ ] Erico Silva e Jose Carlos Esteca Junior devem entrar em contato com Ridec para ele dar um overview da arquitetura.

*Revise as anotações do Gemini para checar se estão corretas. [Confira dicas e saiba como o Gemini faz anotações](https://support.google.com/meet/answer/14754931)*

*Envie feedback sobre o uso do Gemini para criar notas [breve pesquisa.](https://google.qualtrics.com/jfe/form/SV_9vK3UZEaIQKKE7A?confid=ERI3EUpzVik8hxf2oosaDxIXOAIIigIgABgBCA&detailid=standard)*