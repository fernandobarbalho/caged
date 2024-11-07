library(archive)
library(tidyverse)


#Laboratórios

url<- "ftp://ftp.mtps.gov.br/pdet/microdados/"

download.file(url, destfile = "CAGEDEST_raiz.html")

url<- "ftp://ftp.mtps.gov.br/pdet/microdados/NOVO CAGED/Leia-me.txt"

download.file(url, destfile = "Leia-me.txt")


url<- "ftp://ftp.mtps.gov.br/pdet/microdados/NOVO CAGED/2024/202409/CAGEDFOR202409.7z"

download.file(url, destfile = "CAGEDFOR202409.7z")


url<- "ftp://ftp.mtps.gov.br/pdet/microdados/NOVO CAGED/2024/202409/CAGEDMOV202409.7z"

download.file(url, destfile = "CAGEDMOV202409.7z", mode = "wb")

getwd()

dir()

archive_extract("CAGEDMOV202409.7z")

CCAGEDMOV202409 <- read_delim("CAGEDMOV202409.txt", 
                              delim = ";", escape_double = FALSE, locale = locale(decimal_mark = ",", 
                                                                                  grouping_mark = "."), trim_ws = TRUE)

CCAGEDMOV202409 <- janitor::clean_names(CCAGEDMOV202409)

url<- "ftp://ftp.mtps.gov.br/pdet/microdados/NOVO CAGED/"
download.file(url, destfile = "novo_caged_raiz.html")



url<- "ftp://ftp.mtps.gov.br/pdet/microdados/CAGED/2019/CAGEDEST_122019.7z"

download.file(url, destfile = "CAGEDEST_122019.7z")


url<- "ftp://ftp.mtps.gov.br/pdet/microdados/CAGED/2019/CAGEDEST_092024.7z"

download.file(url, destfile = "CAGEDEST_122019.7z")



###Função de ETL

busca_caged <- function(ano, mes, tipo="mov"){
  #movimentação no prazo: CAGEDMOVAAAAMM
  #fora do prazo: CAGEDFORAAAAMM
  #exclusão: CAGEDEXCAAAAMM
  
  url_base<- "ftp://ftp.mtps.gov.br/pdet/microdados/NOVO CAGED/"
  #extensão url: AAAA/AAAAMM/CAGEDFORAAAAMM.7z
  
  arquivo<- paste0("CAGED",tipo,ano,mes)
  
  arquivo_7z<- paste0(arquivo,".7z")
  
  arquivo_txt<- paste0(arquivo,".txt")
  
  url_extensao<- paste0(url_base,ano,"/",ano,mes,"/",arquivo_7z)
  
  download.file(url_extensao,  destfile = arquivo_7z, mode = "wb" )

  archive::archive_extract(arquivo_7z)
  
  df_caged <-readr::read_delim(arquivo_txt, 
                                delim = ";", escape_double = FALSE, locale = locale(decimal_mark = ",", 
                                                                                    grouping_mark = "."), trim_ws = TRUE)
  
  df_caged <- janitor::clean_names(df_caged)
  
  df_caged
}


dados_setembro_2024<- busca_caged("2024","09")
