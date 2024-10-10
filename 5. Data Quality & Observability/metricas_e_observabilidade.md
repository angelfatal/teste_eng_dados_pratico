  ***Obs: Código com as verificações de Data Quality estão no script na pasta "1. ETL e Manipulação de Dados"***


## MÉTRICAS DE OBSERVABILIDADE


***1 . Monitore o tempo que leva para os dados serem extraídos, transformados e carregados.***


Métricas de tempo de execução:


   - Tempo de extração
   - Tempo de transformação
   - Tempo de carga


Exemplo de ferramenta que pode ser utilizada: AWS CloudWatch


Definição de métricas de SLA que define tempos aceitáveis para cada um dos estágios.


**Monitoramento:**
- Podemos utilizar os logs gerados para cada processo e criar alertas quando exceder as métricas de SLA.
- Criação de um dashboard para acompanhamento da evolução dos tempos de execução ao longo do período
-
***2. Implemente alertas para qualquer falha ou anomalia durante o processo ETL.***


Métricas e monitoramento dos Dados


 - Disponibilidade: verificação se os dados estão disponíveis
 - Tempestividade: verificação se o dado está atualizado e disponível em tempo hábil.
 - Consistência e Uniformidade: Validação se os dados são do tipo esperado e se estão de acordo com regras de integridade. Verificação de duplicidade.
 - Completude: Volume de dados recebidos e ingeridos (utilização de zscore para definições de intervalos e valores históricos). Validação se possui todas as linhas e colunas esperadas.
 - Acurácia e validade: detecção de anomalias nos valores (como valores negativos ou nulos)
 
Métricas e monitoramento dos processos ETL


 - Falha na leitura dos arquivos recebidos
 - Falha na conexão com a api/origem
 - Falha na conexão com o banco de dados de destino
 - Anomalias nos tempos de execução
 - Monitoramento dos recursos de processamento das ferramentas utilizadas


Definição de métricas de SLA que definem os valores aceitáveis para cada um dos estágios.


**Monitoramento:**
- Podemos utilizar os logs gerados para cada processo e criar alertas quando exceder as métricas de SLA.
- Criação de um dashboard para acompanhamento de todas as métricas ao longo do tempo


Exemplo de ferramenta que podem ser utilizadas: AWS CloudWatch (monitoramento de logs) e AWS Glue Data Quality (monitoramento dos dados durante processamento)


***3. Descreva como você rastrearia um problema no pipeline, desde o alerta até a fonte do problema.***


Etapas do rastreamento:
 


 1. Receber alerta

 Um alerta é recebido via email ou outra ferramenta de comunicação integrada (como o slack por exemplo) ou detecção de problema através de visualização de um dashboard de acompanhamento das métricas.
 Análise inicial entendo se é uma ocorrência única ou recorrente.


2. Análise de logs

Após alerta, analisar os logs dos processos para entender onde exatamente pode ter acontecido o problema e qual foi o problema.
Verificar as métricas aqui também é bastante útil para entender o impacto e o comportamento do pipeline antes e durante a falha, como por exemplo os tempos de execução ou volumetria de dados.


3. Reprodução e Investigação

Após avaliação do problema, podemos tentar reproduzir o problema com um conjunto de dados menor ou simular a fase que falhou (extração, transformação ou carga). Isso pode ser feito em um ambiente de desenvolvimento para testar correções.


4. Correções

Com base na análise anterior, entendemos o problema e aplicamos as correções necessárias, testando em ambiente de desenvolvimento.
Uma vez, concluída as correções e testes, subimos para o ambiente de produção.


5. Monitoramento contínuo e Revisão das métricas

Após aplicar as correções, re-executamos o pipeline ETL e continuamos monitorando as métricas e logs. Caso os problemas tenham sido resolvidos, encerramos o incidente e seguimos com o monitoramento para garantir que o problema não ocorra novamente.
Aqui pode ser necessária revisão das métricas.

