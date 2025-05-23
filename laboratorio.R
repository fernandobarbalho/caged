library(archive)
library(tidyverse)

###Crie a função busca_caged a partir do script ETL.R

### GEração de emprego do setor varejista no governo Lula

### DAdos de 2023

geracao_2023<- 
  map_dfr(c("01","02","03","04","05","06","07","08","09","10","11","12"), function(a_mes){ #
    print(a_mes)
    dados_caged<- busca_caged("2023",a_mes)
    
    if (is.null(dados_caged)) return()
    
    
    saldo<-
    (dados_caged%>%
      filter(secao == "G",
             str_sub(as.character(subclasse),1,2)=="47") %>%
      summarise(saldo_total = sum(saldomovimentacao)))$saldo_total
    
    tibble(ano = 2023, mes= a_mes, saldo =  saldo )
    
  })

geracao_2024<- 
  map_dfr(c("01","02","03","04","05","06","07","08","09"), function(a_mes){ #
    print(a_mes)
    dados_caged<- busca_caged("2024",a_mes)
    
    if (is.null(dados_caged)) return()
    
    
    saldo<-
      (dados_caged%>%
         filter(secao == "G",
                str_sub(as.character(subclasse),1,2)=="47") %>%
         summarise(saldo_total = sum(saldomovimentacao)))$saldo_total
    
    tibble(ano = 2024, mes= a_mes, saldo =  saldo )
    
  })

#teste git

