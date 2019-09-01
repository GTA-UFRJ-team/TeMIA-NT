# Stream

Stream é um pacote em [Scala](https://scala-lang.org/) contendo implementações de métodos de detecção de intrusão em redes e de deteção de anomalias através da classificação de fluxos por meio do uso de algoritmos de aprendizado de máquina. Os algoritmos são executados usando o [Apache Spark](https://spark.apache.org/), um *framework* de processamento paralelo e distribuído.

Por enquanto, os modelos de treinamento (e as classificações) são construídos (e realizadas) a partir de um *dataset* do [Grupo de Teleinformática e Automação](https://www.gta.ufrj.br/) (GTA) da [COPPE](http://www.coppe.ufrj.br/)/[Universidade Federal do Rio de Janeiro](https://ufrj.br/) (UFRJ), mas facilmente pode ser adaptado para utilizar outros *datasets*.

## Como usar

Para rodar as simulações, basta enviar ao nó *master* do Spark o *Object* correspondente ao algoritmo desejado:

`spark-submit --master <master> --class offline.<object> <arquivo jar> <parâmetros>`

O arquivo *jar* do código a ser utilizado pelo Spark pode ser gerado pela ferramenta [Scala Build Tool](https://www.scala-sbt.org/) (SBT). Para isso, basta acessar o diretório do projeto, e executar o comando `sbt`. O ambiente dessa ferramenta será carregado e, para gerar o arquivo *jar*, deve-se executar o comando `package` dentro desse ambiente. Ao final desse processo, o arquivo *jar* poderá ser encontrado no diretório `target/scala-<scalaversion>/` com o nome `stream_<scalaversion>-0.1.0-SNAPSHOT.jar`.

Os métodos de aprendizado de máquina já são implementados pela *library* `mllib` do Spark, onde apenas alguns parâmetros de cada método são utilizados.

Abaixo, são listados os métodos já prontos para uso e seus parâmetros permitidos:

### Árvore de decisão

Scala *Object*: `DecisionTree`

Parâmetros:

Tipo | Descrição |
| :---: | :--- |
| `File` | Arquivo CSV do dataset para treino e validação |
| `File` | Arquivo CSV que conterá as métricas calculadas |
| `Integer` | Número de simulações |
| `Integer` | Número de núcleos de processamento utilizados para a simulação |
| `String` | Critério usado para cálculo do ganho de informação. Aceita "gini" ou "entropy" |
| `Integer` | Profundidade máxima da árvore (>= 0) |
| `Integer` | Número de componentes selecionados após aplicação do PCA. Opcional |

### Floresta aleatória

Scala *Object*: `RandomForest`

Parâmetros:

Tipo | Descrição |
| :---: | :--- |
| `File` | Arquivo CSV do dataset para treino e validação |
| `File` | Arquivo CSV que conterá as métricas calculadas |
| `Integer` | Número de simulações |
| `Integer` | Número de núcleos de processamento utilizados para a simulação |
| `Integer` | Número de árvores para treinar (>= 1) |
| `String` | Tipo de impureza de todas as árvores. Aceita "gini" ou "entropy" |
| `Integer` | Profundidade máxima de todas as árvore |
| `Integer` | Número de componentes selecionados após aplicação do PCA. Opcional |

### Rede neural

Scala *Object*: `NeuralNetwork`

Parâmetros:

Tipo | Descrição |
| :---: | :--- |
| `File` | Arquivo CSV do dataset para treino e validação |
| `File` | Arquivo CSV que conterá as métricas calculadas |
| `Integer` | Número de simulações |
| `Integer` | Número de núcleos de processamento utilizados para a simulação |
| `String` | Tamanho de cada camada da rede neural, incluindo as camadas de entrada e saída. A *String* precisa ser um conjunto de números inteiros separados por vírgula |
| `Integer` | Número máximo de iterações (>= 0) |
| `Integer` | Número de componentes selecionados após aplicação do PCA. Opcional |

### Regressão logística

Scala *Object*: `LogisticRegression`

Parâmetros:

Tipo | Descrição |
| :---: | :--- |
| `File` | Arquivo CSV do dataset para treino e validação |
| `File` | Arquivo CSV que conterá as métricas calculadas |
| `Integer` | Número de simulações |
| `Integer` | Número de núcleos de processamento utilizados para a simulação |
| `Double` | Parâmetro de regularização (>= 0) |
| `Double` | Parâmetro para a ElasticNet [0, 1] |
| `Integer` | Número máximo de iterações (>= 0) |
| `Integer` | Número de componentes selecionados após aplicação do PCA. Opcional |

### Support Vector Machine

Scala *Object*: `SupportVectorMachine`

Parâmetros:

Tipo | Descrição |
| :---: | :--- |
| `File` | Arquivo CSV do dataset para treino e validação |
| `File` | Arquivo CSV que conterá as métricas calculadas |
| `Integer` | Número de simulações |
| `Integer` | Número de núcleos de processamento utilizados para a simulação |
| `Double` | Parâmetro de regularização (>= 0) |
| `Integer` | Número máximo de iterações (>= 0) |
| `Integer` | Número de componentes selecionados após aplicação do PCA. Opcional |

### Média e variância (Detecção de anomalia)

Scala *Object*: `MeanVariance`

Parâmetros:

Tipo | Descrição |
| :---: | :--- |
| `File` | Arquivo CSV do dataset para treino |
| `File` | Arquivo CSV do dataset para validação |
| `File` | Arquivo CSV que conterá as métricas calculadas |
| `Integer` | Número de simulações |
| `Integer` | Número de núcleos de processamento utilizados para a simulação |
| `Double` | Limiar aplicado à variância para definir os limites inferior e superior de caracterização de um fluxo como legítimo |
| `Integer` | Número de componentes selecionados após aplicação do PCA. Opcional |

## Referências

1. http://spark.apache.org/docs/2.4.0/ml-guide.html
1. http://spark.apache.org/docs/2.4.0/api/scala/
