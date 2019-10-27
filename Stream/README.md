# Stream

Stream é um pacote em [Scala][1] contendo implementações de métodos de detecção de intrusão em redes e de deteção de anomalias através da classificação de fluxos (abstrações de pacotes de rede) por meio do uso de algoritmos de aprendizado de máquina. Os algoritmos são executados usando o [Apache Spark][2], um *framework* de processamento paralelo e distribuído.

Os modelos de treinamento (e as classificações) são construídos (e realizadas) a partir de *datasets* contendo *features* [definidas][3] pelo [Grupo de Teleinformática e Automação][4] (GTA) da [COPPE][5]/[Universidade Federal do Rio de Janeiro][6] (UFRJ) ou definidas pelo programa de abstração em fluxos [Flowtbag][7], mas facilmente pode ser adaptado para utilizar outras *features*.

## Como usar

Para rodar as simulações, basta enviar ao nó *master* do Spark o *Object* correspondente ao algoritmo desejado:

`spark-submit --master <master> --class offline.<object> <arquivo jar> <parâmetros>`

O arquivo *jar* do código a ser utilizado pelo Spark pode ser gerado pela ferramenta [Scala Build Tool][8] (SBT). Para isso, basta acessar o diretório do projeto, e executar o comando `sbt`. O ambiente dessa ferramenta será carregado e, para gerar o arquivo *jar*, deve-se executar o comando `package` dentro desse ambiente. Ao final desse processo, o arquivo *jar* poderá ser encontrado no diretório `target/scala-<scalaversion>/` com o nome `stream_<scalaversion>-0.1.0-SNAPSHOT.jar`.

Os métodos de aprendizado de máquina já são implementados pela *library* `mllib` do Spark, onde apenas alguns parâmetros de cada método são utilizados.

É possível, também, rodar simulações de forma que o modelo seja gerado de forma *offline* e a classificação dos fluxos de forma *online*, utilizando a nova *library* `streaming` do Spark (Structured Streaming):

`spark-submit --master <master> --class online.<object> <arquivo jar> <parâmetros>`

Nos casos anteriores, o programa faz a leitura de arquivos CSV contendo as abstrações dos pacotes em fluxos contendo as *features*, mas é possível utilizar o [Kafka][9] para receber essas abstrações de forma *online*:

`spark-submit --master <master> --packages org.apache.spark:spark-sql-kafka-<versao_kafka>:<versao_spark> --class onlineKafka.<object> <arquivo jar> <parâmetros>`

Abaixo, são listados os métodos já prontos para uso e seus parâmetros permitidos:

### Árvore de decisão

Scala *Object*: `DecisionTree`

Parâmetros (offline):

Tipo | Descrição |
| :---: | :--- |
| `File` | Arquivo CSV do dataset para treino e classificação |
| `File` | Arquivo CSV que conterá as métricas calculadas |
| `Integer` | Número de simulações |
| `Integer` | Número de núcleos de processamento utilizados para a simulação |
| `String` | Critério usado para cálculo do ganho de informação. Aceita "gini" ou "entropy" |
| `Integer` | Profundidade máxima da árvore (>= 0) |
| `Integer` | Número de componentes selecionados após aplicação do PCA. Opcional |

Parâmetros (online):

Tipo | Descrição |
| :---: | :--- |
| `File` | Arquivo CSV do dataset para treino |
| `Integer` | Tempo de *timeout* para a consulta em *streaming* encerrar |
| `Path` | Diretório com arquivos CSV para classificação |
| `Path` | Diretório para armazenar os arquivos CSV que conterão o resultado das classificações junto com os rótulos originais |
| `File` | Arquivo CSV que conterá as métricas de *streaming* calculadas |
| `File` | Arquivo CSV que conterá as métricas calculadas |
| `String` | Critério usado para cálculo do ganho de informação. Aceita "gini" ou "entropy" |
| `Integer` | Profundidade máxima da árvore (>= 0) |
| `Integer` | Número de componentes selecionados após aplicação do PCA. Opcional |

Parâmetros (online Kafka):

Tipo | Descrição |
| :---: | :--- |
| `File` | Arquivo CSV do dataset para treino |
| `Integer` | Tempo de *timeout* para a consulta em *streaming* encerrar |
| `String` | *host* e porta do serviço do Kafka |
| `String` | Nome do tópico do Kafka que contém os fluxos |
| `Path` | Diretório para armazenar os arquivos CSV que conterão o resultado das classificações junto com os rótulos originais |
| `File` | Arquivo CSV que conterá as métricas de *streaming* calculadas |
| `File` | Arquivo CSV que conterá as métricas calculadas |
| `String` | Critério usado para cálculo do ganho de informação. Aceita "gini" ou "entropy" |
| `Integer` | Profundidade máxima da árvore (>= 0) |
| `Integer` | Número de componentes selecionados após aplicação do PCA. Opcional |

### Floresta aleatória

Scala *Object*: `RandomForest`

Parâmetros:

Tipo | Descrição |
| :---: | :--- |
| `File` | Arquivo CSV do dataset para treino e classificação |
| `File` | Arquivo CSV que conterá as métricas calculadas |
| `Integer` | Número de simulações |
| `Integer` | Número de núcleos de processamento utilizados para a simulação |
| `Integer` | Número de árvores para treinar (>= 1) |
| `String` | Tipo de impureza de todas as árvores. Aceita "gini" ou "entropy" |
| `Integer` | Profundidade máxima de todas as árvore |
| `Integer` | Número de componentes selecionados após aplicação do PCA. Opcional |

Parâmetros (online):

Tipo | Descrição |
| :---: | :--- |
| `File` | Arquivo CSV do dataset para treino |
| `Integer` | Tempo de *timeout* para a consulta em *streaming* encerrar |
| `Path` | Diretório com arquivos CSV para classificação |
| `Path` | Diretório para armazenar os arquivos CSV que conterão o resultado das classificações junto com os rótulos originais |
| `File` | Arquivo CSV que conterá as métricas de *streaming* calculadas |
| `File` | Arquivo CSV que conterá as métricas calculadas |
| `Integer` | Número de árvores para treinar (>= 1) |
| `String` | Tipo de impureza de todas as árvores. Aceita "gini" ou "entropy" |
| `Integer` | Profundidade máxima de todas as árvore |
| `Integer` | Número de componentes selecionados após aplicação do PCA. Opcional |

Parâmetros (online Kafka):

Tipo | Descrição |
| :---: | :--- |
| `File` | Arquivo CSV do dataset para treino |
| `Integer` | Tempo de *timeout* para a consulta em *streaming* encerrar |
| `String` | *host* e porta do serviço do Kafka |
| `String` | Nome do tópico do Kafka que contém os fluxos |
| `Path` | Diretório para armazenar os arquivos CSV que conterão o resultado das classificações junto com os rótulos originais |
| `File` | Arquivo CSV que conterá as métricas de *streaming* calculadas |
| `File` | Arquivo CSV que conterá as métricas calculadas |
| `Integer` | Número de árvores para treinar (>= 1) |
| `String` | Tipo de impureza de todas as árvores. Aceita "gini" ou "entropy" |
| `Integer` | Profundidade máxima de todas as árvore |
| `Integer` | Número de componentes selecionados após aplicação do PCA. Opcional |

### Rede neural

Scala *Object*: `NeuralNetwork`

Parâmetros:

Tipo | Descrição |
| :---: | :--- |
| `File` | Arquivo CSV do dataset para treino e classificação |
| `File` | Arquivo CSV que conterá as métricas calculadas |
| `Integer` | Número de simulações |
| `Integer` | Número de núcleos de processamento utilizados para a simulação |
| `String` | Tamanho de cada camada da rede neural, incluindo as camadas de entrada e saída. A *String* precisa ser um conjunto de números inteiros separados por vírgula |
| `Integer` | Número máximo de iterações (>= 0) |
| `Integer` | Número de componentes selecionados após aplicação do PCA. Opcional |

Parâmetros (online):

Tipo | Descrição |
| :---: | :--- |
| `File` | Arquivo CSV do dataset para treino |
| `Integer` | Tempo de *timeout* para a consulta em *streaming* encerrar |
| `Path` | Diretório com arquivos CSV para classificação |
| `Path` | Diretório para armazenar os arquivos CSV que conterão o resultado das classificações junto com os rótulos originais |
| `File` | Arquivo CSV que conterá as métricas de *streaming* calculadas |
| `File` | Arquivo CSV que conterá as métricas calculadas |
| `String` | Tamanho de cada camada da rede neural, incluindo as camadas de entrada e saída. A *String* precisa ser um conjunto de números inteiros separados por vírgula |
| `Integer` | Número máximo de iterações (>= 0) |
| `Integer` | Número de componentes selecionados após aplicação do PCA. Opcional |

Parâmetros (online Kafka):

Tipo | Descrição |
| :---: | :--- |
| `File` | Arquivo CSV do dataset para treino |
| `Integer` | Tempo de *timeout* para a consulta em *streaming* encerrar |
| `String` | *host* e porta do serviço do Kafka |
| `String` | Nome do tópico do Kafka que contém os fluxos |
| `Path` | Diretório para armazenar os arquivos CSV que conterão o resultado das classificações junto com os rótulos originais |
| `File` | Arquivo CSV que conterá as métricas de *streaming* calculadas |
| `File` | Arquivo CSV que conterá as métricas calculadas |
| `String` | Tamanho de cada camada da rede neural, incluindo as camadas de entrada e saída. A *String* precisa ser um conjunto de números inteiros separados por vírgula |
| `Integer` | Número máximo de iterações (>= 0) |
| `Integer` | Número de componentes selecionados após aplicação do PCA. Opcional |

### Regressão logística

Scala *Object*: `LogisticRegression`

Parâmetros:

Tipo | Descrição |
| :---: | :--- |
| `File` | Arquivo CSV do dataset para treino e classificação |
| `File` | Arquivo CSV que conterá as métricas calculadas |
| `Integer` | Número de simulações |
| `Integer` | Número de núcleos de processamento utilizados para a simulação |
| `Double` | Parâmetro de regularização (>= 0) |
| `Double` | Parâmetro para a ElasticNet [0, 1] |
| `Integer` | Número máximo de iterações (>= 0) |
| `Integer` | Número de componentes selecionados após aplicação do PCA. Opcional |

Parâmetros (online):

Tipo | Descrição |
| :---: | :--- |
| `File` | Arquivo CSV do dataset para treino |
| `Integer` | Tempo de *timeout* para a consulta em *streaming* encerrar |
| `Path` | Diretório com arquivos CSV para classificação |
| `Path` | Diretório para armazenar os arquivos CSV que conterão o resultado das classificações junto com os rótulos originais |
| `File` | Arquivo CSV que conterá as métricas de *streaming* calculadas |
| `File` | Arquivo CSV que conterá as métricas calculadas |
| `Double` | Parâmetro de regularização (>= 0) |
| `Double` | Parâmetro para a ElasticNet [0, 1] |
| `Integer` | Número máximo de iterações (>= 0) |
| `Integer` | Número de componentes selecionados após aplicação do PCA. Opcional |

Parâmetros (online Kafka):

Tipo | Descrição |
| :---: | :--- |
| `File` | Arquivo CSV do dataset para treino |
| `Integer` | Tempo de *timeout* para a consulta em *streaming* encerrar |
| `String` | *host* e porta do serviço do Kafka |
| `String` | Nome do tópico do Kafka que contém os fluxos |
| `Path` | Diretório para armazenar os arquivos CSV que conterão o resultado das classificações junto com os rótulos originais |
| `File` | Arquivo CSV que conterá as métricas de *streaming* calculadas |
| `File` | Arquivo CSV que conterá as métricas calculadas |
| `Double` | Parâmetro de regularização (>= 0) |
| `Double` | Parâmetro para a ElasticNet [0, 1] |
| `Integer` | Número máximo de iterações (>= 0) |
| `Integer` | Número de componentes selecionados após aplicação do PCA. Opcional |

### Support Vector Machine

Scala *Object*: `SupportVectorMachine`

Parâmetros:

Tipo | Descrição |
| :---: | :--- |
| `File` | Arquivo CSV do dataset para treino e classificação |
| `File` | Arquivo CSV que conterá as métricas calculadas |
| `Integer` | Número de simulações |
| `Integer` | Número de núcleos de processamento utilizados para a simulação |
| `Double` | Parâmetro de regularização (>= 0) |
| `Integer` | Número máximo de iterações (>= 0) |
| `Integer` | Número de componentes selecionados após aplicação do PCA. Opcional |

Parâmetros (online):

Tipo | Descrição |
| :---: | :--- |
| `File` | Arquivo CSV do dataset para treino |
| `Integer` | Tempo de *timeout* para a consulta em *streaming* encerrar |
| `Path` | Diretório com arquivos CSV para classificação |
| `Path` | Diretório para armazenar os arquivos CSV que conterão o resultado das classificações junto com os rótulos originais |
| `File` | Arquivo CSV que conterá as métricas de *streaming* calculadas |
| `File` | Arquivo CSV que conterá as métricas calculadas |
| `Double` | Parâmetro de regularização (>= 0) |
| `Integer` | Número máximo de iterações (>= 0) |
| `Integer` | Número de componentes selecionados após aplicação do PCA. Opcional |

Parâmetros (online Kafka):

Tipo | Descrição |
| :---: | :--- |
| `File` | Arquivo CSV do dataset para treino |
| `Integer` | Tempo de *timeout* para a consulta em *streaming* encerrar |
| `String` | *host* e porta do serviço do Kafka |
| `String` | Nome do tópico do Kafka que contém os fluxos |
| `Path` | Diretório para armazenar os arquivos CSV que conterão o resultado das classificações junto com os rótulos originais |
| `File` | Arquivo CSV que conterá as métricas de *streaming* calculadas |
| `File` | Arquivo CSV que conterá as métricas calculadas |
| `Double` | Parâmetro de regularização (>= 0) |
| `Integer` | Número máximo de iterações (>= 0) |
| `Integer` | Número de componentes selecionados após aplicação do PCA. Opcional |

### Média e variância (para detecção de anomalia)

Scala *Object*: `MeanVariance`

Parâmetros:

Tipo | Descrição |
| :---: | :--- |
| `File` | Arquivo CSV do dataset para treino |
| `Path` | Diretório com arquivos CSV para classificação |
| `File` | Arquivo CSV que conterá as métricas calculadas |
| `Integer` | Número de simulações |
| `Integer` | Número de núcleos de processamento utilizados para a simulação |
| `Double` | Limiar aplicado à variância para definir os limites inferior e superior de caracterização de um fluxo como legítimo |
| `Integer` | Número de componentes selecionados após aplicação do PCA. Opcional |

Parâmetros (online):

Tipo | Descrição |
| :---: | :--- |
| `File` | Arquivo CSV do dataset para treino |
| `Integer` | Tempo de *timeout* para a consulta em *streaming* encerrar |
| `Path` | Diretório com arquivos CSV para classificação |
| `Path` | Diretório para armazenar os arquivos CSV que conterão o resultado das classificações junto com os rótulos originais |
| `File` | Arquivo CSV que conterá as métricas de *streaming* calculadas |
| `File` | Arquivo CSV que conterá as métricas calculadas |
| `String` | Critério usado para cálculo do ganho de informação. Aceita "gini" ou "entropy" |
| `Integer` | Profundidade máxima da árvore (>= 0) |
| `Integer` | Número de componentes selecionados após aplicação do PCA. Opcional |

Parâmetros (online Kafka):

Tipo | Descrição |
| :---: | :--- |
| `File` | Arquivo CSV do dataset para treino |
| `Integer` | Tempo de *timeout* para a consulta em *streaming* encerrar |
| `String` | *host* e porta do serviço do Kafka |
| `String` | Nome do tópico do Kafka que contém os fluxos |
| `Path` | Diretório para armazenar os arquivos CSV que conterão o resultado das classificações junto com os rótulos originais |
| `File` | Arquivo CSV que conterá as métricas de *streaming* calculadas |
| `File` | Arquivo CSV que conterá as métricas calculadas |
| `String` | Critério usado para cálculo do ganho de informação. Aceita "gini" ou "entropy" |
| `Integer` | Profundidade máxima da árvore (>= 0) |
| `Integer` | Número de componentes selecionados após aplicação do PCA. Opcional |

[1]: http://spark.apache.org/docs/latest/ml-guide.html
[2]: http://spark.apache.org/docs/latest/api/scala/
[3]: https://www.gta.ufrj.br/ftp/gta/TechReports/Antonio17.pdf
[4]: https://www.gta.ufrj.br/
[5]: http://www.coppe.ufrj.br/
[6]: https://ufrj.br/
[7]: https://www.scala-sbt.org/
[8]: https://github.com/danielarndt/flowtbag
[9]: https://kafka.apache.org/
