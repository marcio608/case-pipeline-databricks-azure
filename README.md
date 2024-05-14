![imagem](https://github.com/marcio608/case-pipeline-databricks-azure/blob/main/Azure-Data-Factory-cover.png)




# **Case: Criando uma pipeline usando Databricks e Microsoft Azure**

### **Objetivo**

#### A idea desse case é simular um pipeline de dados utilizando o Azure Data Factory para a orquestração e monitoração do pipe e o Databricks para criar os notebooks para fazer as transformações necessárias.
#### A figura abaixo mostra como irá funcionar o pipeline.

!(imagem_pipeline)[https://github.com/marcio608/case-pipeline-databricks-azure/blob/main/pipeline_photo.png]


### **Base de dados**

#### Neste estudo usou-se uma base dados imobiliários da região do Rio de Janeiro. Os dados estão no formado JSON.


### **Microsoft Azure**

#### * Criação de grupo de recursos para o case.
#### * Criação de um datalake (conta de armazenamento).
#### * Criação das camadas de armazenamento no datalake.
##### 1 imoveis
##### 1.1 inbound
##### 1.2 bronze
##### 1.3 silver

#### * Criação o registro de aplicativos para fazer a conexão entre o Databricks e o datalake.
#### * Usar o IAM e o ACL para dar as permissões.


### **Databricks**

#### * A plataforma está presente nos principais serviços de nuvem (Azure, GCP e AWS).
#### * Criar um workspace.
#### * Habilitar o DBFS
#### * Fazer a integração do Databricks e o Github. 





