library(tidyverse)
library(httr)
library(janitor)
library(getPass)
library(repr)
library(data.table)
library(readr)
library(dplyr)
library(DescTools)

token = getPass()  #Token de acesso á API da PCDaS 

url_base = "https://bigdata-api.fiocruz.br"

convertRequestToDF <- function(request){
  variables = unlist(content(request)$columns)
  variables = variables[names(variables) == "name"]
  column_names <- unname(variables)
  values = content(request)$rows
  df <- as.data.frame(do.call(rbind,lapply(values,function(r) {
    row <- r
    row[sapply(row, is.null)] <- NA
    rbind(unlist(row))
  } )))
  names(df) <- column_names
  return(df)
}

estados <- c('RO','AC','AM','RR','PA','AP','TO','MA','PI','CE','RN','PB','PE','AL','SE','BA','MG','ES','RJ','SP','PR','SC','RS','MS','MT','GO','DF')
endpoint <- paste0(url_base,"/","sql_query")

## Óbitos maternos oficiais dos anos de 2010 a 2021
df_obitos_maternos_aux <- dataframe <- data.frame()

for (estado in estados){
  
  params = paste0('{
      "token": {
        "token": "',token,'"
      },
      "sql": {
        "sql": {"query":" SELECT ano_obito, DTNASC, DTOBITO, def_raca_cor, def_est_civil, ESC2010, OCUP, res_MUNNOME, res_SIGLA_UF, def_loc_ocor, ocor_MUNNOME, ocor_SIGLA_UF, idade_obito_anos, OBITOGRAV, OBITOPUERP, def_assist_med, def_necropsia, CAUSABAS, causabas_capitulo, causabas_categoria, FONTEINV, COUNT(1)',
                  ' FROM \\"datasus-sim\\"',
                  ' WHERE ano_obito >= 2010 AND (res_SIGLA_UF = \'',estado,'\' AND SEXO = 2 AND',
                  ' ((CAUSABAS >= \'O000\'  AND  CAUSABAS <= \'O959\') OR',
                  ' (CAUSABAS >= \'O980\'  AND  CAUSABAS <= \'O999\') OR',
                  ' (CAUSABAS = \'A34\' AND OBITOPUERP != 2) OR',
                  ' ((CAUSABAS >= \'B200\'  AND  CAUSABAS <= \'B249\') AND (OBITOGRAV = 1 OR OBITOPUERP = 1)) OR',
                  ' (CAUSABAS = \'D392\' AND (OBITOGRAV = 1 OR OBITOPUERP = 1)) OR',
                  ' (CAUSABAS = \'E230\' AND (OBITOGRAV = 1 OR OBITOPUERP = 1)) OR',
                  ' ((CAUSABAS >= \'F530\'  AND  CAUSABAS <= \'F539\') AND (OBITOPUERP != 2 OR OBITOPUERP = \' \')) OR',
                  ' (CAUSABAS = \'M830\' AND OBITOPUERP != 2)))',
                  ' GROUP BY ano_obito, DTNASC, DTOBITO, def_raca_cor, def_est_civil, ESC2010, OCUP, res_MUNNOME, res_SIGLA_UF, def_loc_ocor, ocor_MUNNOME, ocor_SIGLA_UF, idade_obito_anos, OBITOGRAV, OBITOPUERP, def_assist_med, def_necropsia, CAUSABAS, causabas_capitulo, causabas_categoria, FONTEINV",
                        "fetch_size": 65000}
      }
    }')
  
  request <- POST(url = endpoint, body = params, encode = "form")
  dataframe <- convertRequestToDF(request)
  names(dataframe) <- c("ano_obito", "data_nasc", "data_obito", "raca_cor", "estado_civil", "escolaridade", "ocupacao", "res_munnome", "res_sigla_uf", "local_de_ocorrencia", "ocor_munnome", "ocor_sigla_uf", "idade", "obitograv", "obitopuerp", "assistencia_medica", "necropsia", "causabas", "causabas_capitulo", "causabas_categoria", "fonteinv", "obitos")
  df_obitos_maternos_aux <- rbind(df_obitos_maternos_aux, dataframe)
  
  repeat {
    
    cursor <- content(request)$cursor
    
    params = paste0('{
          "token": {
            "token": "',token,'"
          },
          "sql": {
            "sql": {"query":" SELECT ano_obito, DTNASC, DTOBITO, def_raca_cor, def_est_civil, ESC2010, OCUP, res_MUNNOME, res_SIGLA_UF, def_loc_ocor, ocor_MUNNOME, ocor_SIGLA_UF, idade_obito_anos, OBITOGRAV, OBITOPUERP, def_assist_med, def_necropsia, CAUSABAS, causabas_capitulo, causabas_categoria, FONTEINV, COUNT(1)',
                    ' FROM \\"datasus-sim\\"',
                    ' WHERE ano_obito >= 2010 AND (res_SIGLA_UF = \'',estado,'\' AND SEXO = 2 AND',
                    ' ((CAUSABAS >= \'O000\'  AND  CAUSABAS <= \'O959\') OR',
                    ' (CAUSABAS >= \'O980\'  AND  CAUSABAS <= \'O999\') OR',
                    ' (CAUSABAS = \'A34\' AND OBITOPUERP != 2) OR',
                    ' ((CAUSABAS >= \'B200\'  AND  CAUSABAS <= \'B249\') AND (OBITOGRAV = 1 OR OBITOPUERP = 1)) OR',
                    ' (CAUSABAS = \'D392\' AND (OBITOGRAV = 1 OR OBITOPUERP = 1)) OR',
                    ' (CAUSABAS = \'E230\' AND (OBITOGRAV = 1 OR OBITOPUERP = 1)) OR',
                    ' ((CAUSABAS >= \'F530\'  AND  CAUSABAS <= \'F539\') AND (OBITOPUERP != 2 OR OBITOPUERP = \' \')) OR',
                    ' (CAUSABAS = \'M830\' AND OBITOPUERP != 2)))',
                    ' GROUP BY ano_obito, DTNASC, DTOBITO, def_raca_cor, def_est_civil, ESC2010, OCUP, res_MUNNOME, res_SIGLA_UF, def_loc_ocor, ocor_MUNNOME, ocor_SIGLA_UF, idade_obito_anos, OBITOGRAV, OBITOPUERP, def_assist_med, def_necropsia, CAUSABAS, causabas_capitulo, causabas_categoria, FONTEINV",
                            "fetch_size": 65000, "cursor": "',cursor,'"}
                            }
                    }')
    
    
    request <- POST(url = endpoint, body = params, encode = "form")
    
    if (length(content(request)$rows) == 0)
      break
    else print("oi")
    
    request <- POST(url = endpoint, body = params, encode = "form")
    dataframe <- convertRequestToDF(request)
    names(dataframe) <- c("ano_obito", "data_nasc", "data_obito", "raca_cor", "estado_civil", "escolaridade", "ocupacao", "res_munnome", "res_sigla_uf", "local_de_ocorrencia", "ocor_munnome", "ocor_sigla_uf", "idade", "obitograv", "obitopuerp", "assistencia_medica", "necropsia", "causabas", "causabas_capitulo", "causabas_categoria", "fonteinv", "obitos")
    df_obitos_maternos_aux <- rbind(df_obitos_maternos_aux, dataframe)
  }
}

head(df_obitos_maternos_aux)

unique(df_obitos_maternos_aux$obitos)
janitor::get_dupes(df_obitos_maternos_aux)

df_obitos_maternos_aux$data_obito <- format(as.Date(df_obitos_maternos_aux$data_obito, format = "%d%m%Y"), "%d/%m/%Y")
df_obitos_maternos_aux$data_nasc <- format(as.Date(df_obitos_maternos_aux$data_nasc, format = "%d%m%Y"), "%d/%m/%Y")

df_obitos_maternos <- df_obitos_maternos_aux |>
  mutate(
    escolaridade = case_when(
      escolaridade == "0" ~ "Sem escolaridade",
      escolaridade == "1" ~ "Fundamental I",
      escolaridade == "2" ~ "Fundamental II",
      escolaridade == "3" ~ "Médio",
      escolaridade == "4" ~ "Superior incompleto",
      escolaridade == "5" ~ "Superior completo",
      escolaridade == "9" ~ "Ignorado"
    ),
    obito_em_idade_fertil = if_else(
      condition = as.numeric(idade) >= 10 & as.numeric(idade) <= 49,
      true = "Sim",
      false = "Não"
    ),
    tipo_de_morte_materna = if_else(
      condition = (causabas >= "B200" & causabas <= "B249") |
        (causabas >= "O100" & causabas <= "O109") |
        ((causabas >= "O240" & causabas != "O244") & causabas <= "O259") |
        (causabas == "O94") |
        (causabas >= "O980" & causabas <= "O999"),
      true = "Indireta",
      false = if_else(causabas == "O95", true = "Não especificada", false = "Direta")
    ),
    periodo_do_obito = case_when(
      obitograv == "1" & obitopuerp != "1" & obitopuerp != "2" ~ "Durante a gravidez, parto ou aborto",
      obitograv != "1" & obitopuerp == "1" ~ "Durante o puerpério, até 42 dias",
      obitograv != "1" & obitopuerp == "2" ~ "Durante o puerpério, de 43 dias a menos de 1 ano",
      (obitograv == "2" & obitopuerp == "3") | (obitograv == "2" & obitopuerp == "9") | (obitograv == "9" & obitopuerp == "3")  ~ "Não na gravidez ou no puerpério",
      #obitograv == "2" & obitopuerp == "9" ~ "Durante o puerpério, até 1 ano, período não discriminado",
      obitograv == "9" & obitopuerp == "9" ~ "Não informado ou ignorado",
      (obitograv == "1" & obitopuerp == "1") | (obitograv == "1" & obitopuerp == "2") ~ "Período inconsistente"
    ),
    investigacao_cmm = if_else(fonteinv == "1", true = "Sim", false = if_else(fonteinv == "9", true = "Sem informação", false = "Não",  missing = "Sem informação"), missing = "Sem informação"),
    obitos = as.numeric(obitos),
    obito_materno = "Sim",
    .after = idade,
  ) |>
  select(!c(obitograv, obitopuerp, obitos))

#df_obitos_maternos[is.na(df_obitos_maternos)] <- "Ignorado"
janitor::get_dupes(df_obitos_maternos)


##Óbitos de grávidas e puérperas desconsiderados dos anos de 2010 a 2021
df_obitos_desconsiderados_aux <- dataframe <- data.frame()

for (estado in estados){
  
  params = paste0('{
      "token": {
        "token": "',token,'"
      },
      "sql": {
        "sql": {"query":" SELECT ano_obito, DTNASC, DTOBITO, def_raca_cor, def_est_civil, ESC2010, OCUP, res_MUNNOME, res_SIGLA_UF, def_loc_ocor, ocor_MUNNOME, ocor_SIGLA_UF, idade_obito_anos, OBITOGRAV, OBITOPUERP, def_assist_med, def_necropsia, CAUSABAS, causabas_capitulo, causabas_categoria, FONTEINV, COUNT(1)',
                  ' FROM \\"datasus-sim\\"',
                  ' WHERE ano_obito >= 2010 AND (res_SIGLA_UF = \'',estado,'\' AND SEXO = 2 AND (OBITOGRAV = 1 OR OBITOPUERP = 1 OR OBITOPUERP = 2) AND',
                  ' NOT (((CAUSABAS >= \'O000\'  AND  CAUSABAS <= \'O959\') OR',
                  ' (CAUSABAS >= \'O980\'  AND  CAUSABAS <= \'O999\') OR',
                  ' (CAUSABAS = \'A34\' AND OBITOPUERP != 2) OR',
                  ' ((CAUSABAS >= \'B200\'  AND  CAUSABAS <= \'B249\') AND (OBITOGRAV = 1 OR OBITOPUERP = 1)) OR',
                  ' (CAUSABAS = \'D392\' AND (OBITOGRAV = 1 OR OBITOPUERP = 1)) OR',
                  ' (CAUSABAS = \'E230\' AND (OBITOGRAV = 1 OR OBITOPUERP = 1)) OR',
                  ' ((CAUSABAS >= \'F530\'  AND  CAUSABAS <= \'F539\') AND (OBITOPUERP != 2 OR OBITOPUERP = \' \')) OR',
                  ' (CAUSABAS = \'M830\' AND OBITOPUERP != 2))))',
                  ' GROUP BY ano_obito, DTNASC, DTOBITO, def_raca_cor, def_est_civil, ESC2010, OCUP, res_MUNNOME, res_SIGLA_UF, def_loc_ocor, ocor_MUNNOME, ocor_SIGLA_UF, idade_obito_anos, OBITOGRAV, OBITOPUERP, def_assist_med, def_necropsia, CAUSABAS, causabas_capitulo, causabas_categoria, FONTEINV",
                  "fetch_size": 65000}
                  }
                  }')
  
  request <- POST(url = endpoint, body = params, encode = "form")
  dataframe <- convertRequestToDF(request)
  names(dataframe) <- c("ano_obito", "data_nasc", "data_obito", "raca_cor", "estado_civil", "escolaridade", "ocupacao", "res_munnome", "res_sigla_uf", "local_de_ocorrencia", "ocor_munnome", "ocor_sigla_uf", "idade", "obitograv", "obitopuerp", "assistencia_medica", "necropsia", "causabas", "causabas_capitulo", "causabas_categoria", "fonteinv", "obitos")
  df_obitos_desconsiderados_aux <- rbind(df_obitos_desconsiderados_aux, dataframe)
  
  repeat {
    
    cursor <- content(request)$cursor
    
    params = paste0('{
          "token": {
            "token": "',token,'"
          },
          "sql": {
            "sql": {"query":" SELECT ano_obito, DTNASC, DTOBITO, def_raca_cor, def_est_civil, ESC2010, OCUP, res_MUNNOME, res_SIGLA_UF, def_loc_ocor, ocor_MUNNOME, ocor_SIGLA_UF, idade_obito_anos, OBITOGRAV, OBITOPUERP, def_assist_med, def_necropsia, CAUSABAS, causabas_capitulo, causabas_categoria, FONTEINV, COUNT(1)',
                    ' FROM \\"datasus-sim\\"',
                    ' WHERE (ano_obito >= 2010 AND res_SIGLA_UF = \'',estado,'\' AND SEXO = 2 AND (OBITOGRAV = 1 OR OBITOPUERP = 1 OR OBITOPUERP = 2) AND',
                    ' NOT (((CAUSABAS >= \'O000\'  AND  CAUSABAS <= \'O959\') OR',
                    ' (CAUSABAS >= \'O980\'  AND  CAUSABAS <= \'O999\') OR',
                    ' (CAUSABAS = \'A34\' AND OBITOPUERP != 2) OR',
                    ' ((CAUSABAS >= \'B200\'  AND  CAUSABAS <= \'B249\') AND (OBITOGRAV = 1 OR OBITOPUERP = 1)) OR',
                    ' (CAUSABAS = \'D392\' AND (OBITOGRAV = 1 OR OBITOPUERP = 1)) OR',
                    ' (CAUSABAS = \'E230\' AND (OBITOGRAV = 1 OR OBITOPUERP = 1)) OR',
                    ' ((CAUSABAS >= \'F530\'  AND  CAUSABAS <= \'F539\') AND (OBITOPUERP != 2 OR OBITOPUERP = \' \')) OR',
                    ' (CAUSABAS = \'M830\' AND OBITOPUERP != 2))))',
                    ' GROUP BY ano_obito, DTNASC, DTOBITO, def_raca_cor, def_est_civil, ESC2010, OCUP, res_MUNNOME, res_SIGLA_UF, def_loc_ocor, ocor_MUNNOME, ocor_SIGLA_UF, idade_obito_anos, OBITOGRAV, OBITOPUERP, def_assist_med, def_necropsia, CAUSABAS, causabas_capitulo, causabas_categoria, FONTEINV",
                    "fetch_size": 65000, "cursor": "',cursor,'"}
                    }
                    }')
    
    
    request <- POST(url = endpoint, body = params, encode = "form")
    
    if (length(content(request)$rows) == 0)
      break
    else print("oi")
    
    request <- POST(url = endpoint, body = params, encode = "form")
    dataframe <- convertRequestToDF(request)
    names(dataframe) <- c("ano_obito", "data_nasc", "data_obito", "raca_cor", "estado_civil", "escolaridade", "ocupacao", "res_munnome", "res_sigla_uf", "local_de_ocorrencia", "ocor_munnome", "ocor_sigla_uf", "idade", "obitograv", "obitopuerp", "assistencia_medica", "necropsia", "causabas", "causabas_capitulo", "causabas_categoria", "fonteinv", "obitos")
    df_obitos_desconsiderados_aux <- rbind(df_obitos_desconsiderados_aux, dataframe)
  }
}

head(df_obitos_desconsiderados_aux)

df_obitos_desconsiderados_aux$data_obito <- format(as.Date(df_obitos_desconsiderados_aux$data_obito, format = "%d%m%Y"), "%d/%m/%Y")
df_obitos_desconsiderados_aux$data_nasc <- format(as.Date(df_obitos_desconsiderados_aux$data_nasc, format = "%d%m%Y"), "%d/%m/%Y")

unique(df_obitos_desconsiderados_aux$obitos)
janitor::get_dupes(df_obitos_desconsiderados_aux)

df_obitos_desconsiderados_aux2 <- rbind(df_obitos_desconsiderados_aux, df_obitos_desconsiderados_aux[rep(which(df_obitos_desconsiderados_aux$obitos == 2)), ]) |>
  dplyr::select(!c("obitos"))

janitor::get_dupes(df_obitos_desconsiderados_aux2)

df_obitos_desconsiderados <- df_obitos_desconsiderados_aux2 |>
  mutate(
    escolaridade = case_when(
      escolaridade == "0" ~ "Sem escolaridade",
      escolaridade == "1" ~ "Fundamental I",
      escolaridade == "2" ~ "Fundamental II",
      escolaridade == "3" ~ "Médio",
      escolaridade == "4" ~ "Superior incompleto",
      escolaridade == "5" ~ "Superior completo",
      escolaridade == "9" ~ "Ignorado"
    ),
    obito_em_idade_fertil = if_else(
      condition = as.numeric(idade) >= 10 & as.numeric(idade) <= 49,
      true = "Sim",
      false = "Não"
    ),
    tipo_de_morte_materna = "Não se aplica",
    periodo_do_obito = case_when(
      obitograv == "1" & obitopuerp != "1" & obitopuerp != "2" ~ "Durante a gravidez, parto ou aborto",
      obitograv != "1" & obitopuerp == "1" ~ "Durante o puerpério, até 42 dias",
      obitograv != "1" & obitopuerp == "2" ~ "Durante o puerpério, de 43 dias a menos de 1 ano",
      (obitograv == "1" & obitopuerp == "1") | (obitograv == "1" & obitopuerp == "2") ~ "Período inconsistente"
    ),
    investigacao_cmm = if_else(fonteinv == "1", true = "Sim", false = if_else(fonteinv == "9", true = "Sem informação", false = "Não",  missing = "Sem informação"), missing = "Sem informação"),
    obito_materno = "Não",
    .after = idade
  ) |> 
  filter(
    causabas_capitulo != "XX.  Causas externas de morbidade e mortalidade",
    periodo_do_obito %in% c("Durante a gravidez, parto ou aborto", "Durante o puerpério, até 42 dias", "Período inconsistente")
  ) |>
  select(!c(obitograv, obitopuerp))

#df_obitos_desconsiderados[is.na(df_obitos_desconsiderados)] <- "Ignorado"
janitor::get_dupes(df_obitos_desconsiderados)

df_completo <- full_join(df_obitos_maternos, df_obitos_desconsiderados)

df_completo <- df_completo |>
  mutate(
    causabas_recodificada = case_when(
      causabas >= "A000" & causabas <= "A079" ~ "O988",
      causabas %like% "A08%" ~ "O985",
      causabas %like% "A09%" ~ "O988",
      causabas >= "A150" & causabas <= "A199" ~ "O980",
      causabas >= "A240" & causabas <= "A499" ~ "O980",
      causabas >= "A500" & causabas <= "A539" ~ "O981",
      causabas %like% "A54%" ~ "O982",
      causabas >= "A550" & causabas <= "A649" ~ "O983",
      causabas >= "A650" & causabas <= "A799" ~ "O988",
      causabas >= "A800" & causabas <= "B099" ~ "O985",
      causabas >= "B150" & causabas <= "B199" ~ "O984",
      causabas >= "B250" & causabas <= "B349" ~ "O985",
      causabas >= "B350" & causabas <= "B499" ~ "O988",
      causabas >= "B500" & causabas <= "B649" ~ "O986",
      causabas >= "B650" & causabas <= "B889" ~ "O988",
      causabas >= "B890" & causabas <= "B999" ~ "O982",
      causabas >= "C000" & causabas <= "D489" ~ "O998",
      causabas >= "D500" & causabas <= "D649" ~ "O990",
      causabas >= "D65" & causabas <= "D899" ~ "O991",
      causabas >= "E000" & causabas <= "E079" ~ "O992",
      causabas >= "E100" & causabas <= "E149" ~ "O24",
      causabas >= "E15" & causabas <= "E349" ~ "O992",
      causabas >= "E40" & causabas <= "E469" ~ "O25",
      causabas >= "E500" & causabas <= "E899" ~ "O992",
      causabas >= "F000" & causabas <= "F529" ~ "O993",
      causabas >= "F540" & causabas <= "F999" ~ "O993",
      causabas >= "G000" & causabas <= "G999" ~ "O993",
      causabas >= "H000" & causabas <= "H959" ~ "O999",
      causabas >= "I00" & causabas <= "I099" ~ "O994",
      causabas %like% "I10%" ~ "O100",
      causabas %like% "I11%" ~ "O101",
      causabas %like% "I12%" ~ "O102",
      causabas %like% "I13%" ~ "O103",
      causabas %like% "I15%" ~ "O104",
      causabas >= "I200" & causabas <= "I999" ~ "O994",
      causabas >= "J000" & causabas <= "J999" ~ "O995",
      causabas >= "K000" & causabas <= "K669" ~ "O996",
      causabas >= "K700" & causabas <= "K779" ~ "O266",
      causabas >= "K800" & causabas <= "K939" ~ "O996",
      causabas >= "L000" & causabas <= "L999" ~ "O997",
      causabas >= "M000" & causabas <= "M829" ~ "O998",
      causabas >= "M832" & causabas <= "M999" ~ "O998",
      causabas >= "N000" & causabas <= "N079" ~ "O268",
      causabas >= "N10" & causabas <= "N129" ~ "O230",
      causabas >= "N130" & causabas <= "N135" ~ "O268",
      causabas == "N136" ~ "O233",
      causabas >= "N137" & causabas <= "N139" ~ "O268",
      causabas >= "N140" & causabas <= "N150" ~ "O998",
      causabas == "N151" ~ "O230",
      causabas >= "N158" & causabas <= "N159" ~ "O998",
      causabas >= "N170" & causabas <= "N199" ~ "O269",
      causabas >= "N200" & causabas <= "N399" ~ "O998",
      causabas >= "N600" & causabas <= "N649" ~ "O998",
      causabas >= "N800" & causabas <= "N909" ~ "O998",
      causabas >= "Q000" & causabas <= "Q999" ~ "O998",
      causabas == "R730" ~ "O998"
    ),
    .after = causabas
  ) 

df_completo$causabas_recodificada <- ifelse(
  df_completo$obito_materno == "Sim", 
  NA,
  ifelse(
    is.na(df_completo$causabas_recodificada),
    "O95",
    df_completo$causabas_recodificada
  )
)

df_completo <- df_completo |>
  mutate(
    tipo_de_morte_materna = if_else(
      obito_materno == "Sim",
      tipo_de_morte_materna,
      if_else(
        condition = (causabas_recodificada >= "B200" & causabas_recodificada <= "B249") |
          (causabas_recodificada >= "O100" & causabas_recodificada <= "O109") |
          ((causabas_recodificada >= "O240" & causabas_recodificada != "O244") & causabas_recodificada <= "O259") |
          (causabas_recodificada == "O94") |
          (causabas_recodificada >= "O980" & causabas_recodificada <= "O999"),
        true = "Indireta",
        false = if_else(causabas_recodificada == "O95", true = "Não especificada", false = "Direta")
      )
    )
  )

causabas_sem_recodificacao <- df_completo |>
  filter(
    obito_materno == "Não",
    is.na(causabas_recodificada)
  ) |>
  select(causabas, causabas_capitulo, causabas_categoria) |>
  mutate(ocorrencias = 1) |>
  group_by(causabas, causabas_capitulo, causabas_categoria) |>
  summarise(ocorrencias = sum(ocorrencias)) |>
  ungroup()

sort(unique(causabas_sem_recodificacao$causabas))

##Exportando os dados 
write.table(df_completo, 'databases/Obitos_maternos_e_desconsiderados_2010_2021.csv', sep = ",", dec = ".", row.names = FALSE)


##Nascidos vivos dos anos de 2010 a 2021
dataframe <- data.frame()

for (estado in estados){
  
  params = paste0('{
      "token": {
        "token": "',token,'"
      },
      "sql": {
        "sql": {"query": "SELECT res_REGIAO, res_SIGLA_UF, ano_nasc, COUNT(1)',
                  ' FROM \\"datasus-sinasc\\"',
                  ' WHERE (res_SIGLA_UF = \'',estado,'\' AND ano_nasc >= 2010)',
                  ' GROUP BY res_REGIAO, res_SIGLA_UF, ano_nasc",
                        "fetch_size": 65000}
      }
    }')
  
  request <- POST(url = endpoint, body = params, encode = "form")
  df_mun <- convertRequestToDF(request)
  names(df_mun) <- c('regiao', 'uf', 'ano', 'nascidos')
  dataframe <- rbind(dataframe, df_mun)
  
  repeat {
    
    cursor <- content(request)$cursor
    
    params = paste0('{
          "token": {
            "token": "',token,'"
          },
          "sql": {
            "sql": {"query":"SELECT res_REGIAO, res_SIGLA_UF, ano_nasc, COUNT(1)',
                    ' FROM \\"datasus-sinasc\\"',
                    ' WHERE (res_SIGLA_UF = \'',estado,'\' AND ano_nasc >= 2010)',
                    ' GROUP BY res_REGIAO, res_SIGLA_UF, ano_nasc",
                           "fetch_size": 65000, "cursor": "',cursor,'"}
          }
        }')
    
    
    request <- POST(url = endpoint, body = params, encode = "form")
    
    if (length(content(request)$rows) == 0)
      break
    else print("oi")
    
    df_mun <- convertRequestToDF(request)
    names(df_mun) <- c('regiao', 'uf', 'ano', 'nascidos')
    dataframe <- rbind(dataframe, df_mun)
  }
}

head(dataframe)

## Exportando os dados
write.table(dataframe |> select(!regiao), 'databases/Nascimentos_muni2010_2021.csv', sep = ",", dec = ".", row.names = FALSE)