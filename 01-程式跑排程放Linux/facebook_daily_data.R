# === Start ===
# 主程式 facebook_daily_data.R
# 維護者: Vincent Chang
# 更新日期: 2021-12-09
# === End ===

project_name <- 'facebook_daily_data'

# 載入套件
library(rJava)
# Sys.setenv(JAVA_HOME='C:\\Program Files\\Java\\jre1.8.0_291')
library(yaml)            # reading config.yml
library(futile.logger)   # log file
library(futile.options)  # log file
library(mailR)           # send mail
library(RMySQL)          # mySQL
library(magrittr)        # pipeline
library(lubridate)       # date manipulation
library(plyr)            # data manipulation
library(dplyr)           # data manipulation
library(tidyr)           # data manipulation
library(jsonlite)        # json format data
library(httr)            # get api data
# library(stringr)         # parse chr


# 設定執行環境
options(scipen = 20)
options(warn = FALSE)
options(encoding = "UTF-8")


# 存log檔 ==================================================
library(futile.logger)   # log file
library(futile.options)  # log file

# 讀取.yml
library(yaml)   # reading config.yml
# con_Mysql_gameDB <- yaml.load_file("C:/Users/pc072/Desktop/fb_Tag_analysis/Mysql_gameDB.yml")
# mysql_data <- yaml.load_file("C:/Users/pc072/Desktop/R/R_config/all_config.yml")
facebook_daily_config_data <- yaml.load_file("/home/poisson/Program_Schedule/R/R_config/facebook_daily.yml")
mysql_data <- yaml.load_file("/home/poisson/Program_Schedule/R/R_config/all_config.yml")

# 專案路徑
logpath <- "/home/poisson/log紀錄檔/mg_fb_tag_analysis/facebook_daily_data.log"
# logpath <- "C:/Users/pc072/Desktop/log紀錄檔/mg_fb_tag_analysis/facebook_daily_data.log"

# log檔路徑
if (file.exists(logpath) == FALSE){
  file.create(logpath)}
# 啟動log
flog.logger(appender = appender.file(logpath), name = project_name)



# NAtoNULL 函數
NAtoNULL <- function(R_data){
  outputs <- lapply(1:length(R_data), function(col_value){
    if (is.na(R_data[col_value])){
      trans_value <- "NULL"
    } else {
      trans_value <- paste0("'", R_data[col_value],"'")
    }
  }) %>%
    call("paste0", ., collapse = ", ") %>%
    eval %>%
    paste0("(", .,")")
  return(outputs)
}


# # 專案路徑
# root_path <- facebook_daily_config_data$root_path
# # 執行路徑
# workpath <- paste0(c(root_path, mysql_data$work_fold, facebook_daily_config_data$project_name), collapse = "/")
# setwd(workpath)
# # log檔路徑
# logpath <- paste0(c(root_path, project_name, facebook_daily_config_data$project_name, paste0(project_name, ".log")), collapse = "/")
# if (file.exists(logpath) == FALSE){
#   file.create(logpath)
# }
# # 啟動log
# flog.logger(appender = appender.file(logpath), name = project_name)


tryCatch({

  # 程式碼參數
  args <- commandArgs(trailingOnly = TRUE)
  # 執行模式
  run_mode <- dplyr::if_else(is.na(args[1]), "hour", args[1])

  # 設定日期 ==================================================

  # 搭配.sh排程設計
  if(NROW(args) > 0) {
    start_date <- args[2]  %>% as.Date
    end_date   <- args[3]  %>% as.Date
  } else {
    start_date <- Sys.Date() - 2
    end_date   <- Sys.Date()
  }

  # 更新6個月
  # start_date <- "2021-10-18" %>% as.Date
  # end_date   <- "2021-10-25" %>% as.Date


  # 程式執行hash time
  exe_update_time <- Sys.time()
  exe_datetime <- as.character(exe_update_time %>% format(.,  "%Y%m%d%H%M%S"))


  # ----- RMySQL -----
  # 連資料庫
  connect_DB <- dbConnect(RMySQL::MySQL(),
                          host     = mysql_data$db$host,
                          dbname   = mysql_data$db$name,
                          dbport   = mysql_data$db$port,
                          username = mysql_data$db$user,
                          password = mysql_data$db$pass)
  # 設定MySQL連線編碼
  dbSendQuery(connect_DB,'SET NAMES utf8')

  # ===== Work Start =====
  flog.info(paste0(exe_datetime, " ", Sys.Date(), " facebook_daily_data.R, ", "執行模式: ", run_mode, ", 起始日期: ", start_date, ", 結束日期: ", end_date), name = project_name)
  print(paste0(exe_datetime, " ", Sys.Date(), " facebook_daily_data.R, ", "執行模式: ", run_mode, ", 起始日期: ", start_date, ", 結束日期: ", end_date))
  flog.info(paste0(exe_datetime, " start facebook_daily_data ", end_date), name = project_name)
  print(paste0(exe_datetime, " start facebook_daily_data ", end_date))

  # 使用者帳號
  fb_user_id <- facebook_daily_config_data$fb_user
  # api版本
  fb_api <- facebook_daily_config_data$fb_api
  # token
  mobile_access_token <- facebook_daily_config_data$access_token$mobile
  web_access_token <- facebook_daily_config_data$access_token$web


  # 是否執行facebook_exception_error執行  # 23848822822070166
  # campaign_list_SQL
  if (run_mode != "error_exception"){
    campaign_list_SQL <- sprintf("SELECT app_no, date, campaign_id FROM fb_insight_app_status
                                 WHERE date BETWEEN '%s' and '%s' AND code = 'campaign_id';",
                                 start_date, end_date)
  } else {
    campaign_list_SQL <- sprintf("SELECT app_no, date, campaign_id FROM fb_insight_app_status
                                 WHERE date = '%s' AND code = 'daily_data' AND status = 0;",
                                 end_date)
    # campaign_list_SQL <- sprintf("SELECT app_no, date, campaign_id FROM fb_insight_app_status
    #                              WHERE date BETWEEN '%s' and '%s' AND code = 'daily_data' AND status = 0;",
    #                              start_date, end_date)
  }
  # if (run_mode != "error_exception"){
  #   campaign_list_SQL <- sprintf("SELECT F.app_no, F.`date`, F.campaign_id
  #                                 FROM fb_insight_app_status F
  #                                 LEFT JOIN fb_ad_account A ON F.account_id = A.account_id
  #                                 WHERE F.`date` BETWEEN '%s' and '%s' AND F.`code` = 'campaign_id'
  #                                 AND A.account_name NOT LIKE '%%iOS14%%';",
  #                                start_date, end_date)
  # } else {
  #   campaign_list_SQL <- sprintf("SELECT app_no, date, campaign_id FROM fb_insight_app_status F
  #                                 LEFT JOIN fb_ad_account A ON F.account_id = A.account_id
  #                                 WHERE date = '%s' AND code = 'daily_data' AND status = 0
  #                                 AND A.account_name NOT LIKE '%%iOS14%%';",
  #                                end_date)
  # }
  campaign_list <- dbGetQuery(connect_DB, campaign_list_SQL)

  # 若排程執行失敗的Campaign，則寄信通知，並且紀錄Campaign資訊
  if(run_mode == "error_exception"){

    if(nrow(campaign_list) != 0){
      error_exception_campaign_id <- campaign_list
      error_exception_campaign_id$No <- seq(1,length(error_exception_campaign_id$campaign_id))
      error_exception_campaign_id %<>% dplyr::select(c('No','campaign_id'))
      error_exception_campaign_id %<>% apply(., 1, paste0, collapse = "、") %>% paste0(., collapse = "\n")
    } else{error_exception_campaign_id <- "NULL"}

    # 寄信通知
    send.mail(from = mysql_data$mail$from,
              to = mysql_data$mail$to,
              subject = sprintf("%s - facebook_daily_data 啟動新機制-更新_%s-Fail_Campaign" , Sys.time(), end_date),
              body = sprintf("%s ~ %s \n\n 已更新Fail_Campaign：\n %s \n\n\n\n\n\n\n\n\n\n"
                           ,start_date , end_date , error_exception_campaign_id) ,
              encoding = "utf-8",
              smtp = list(host.name = "aspmx.l.google.com", port = 25),
              authenticate = FALSE,
              send = TRUE)
  }

  # 根據fb_insight_app_status AND code = 'campaign_id'之資料表中，若這些campaign有手遊就跑手遊、有頁遊就跑頁遊。
  run_app_no <- campaign_list$app_no %>% unique

  if (length(run_app_no)>0){

    # 一次batch最大使用量(最多50)
    fb_max_request <- facebook_daily_config_data$fb_max_request
    # 紀錄成功的campaign_id資訊
    success_campaign_id <- data.frame(NULL, stringsAsFactors = FALSE)
    # 紀錄失敗的campaign_id資訊
    fail_campaign_id <- data.frame(NULL, stringsAsFactors = FALSE)
    # 紀錄失敗的資訊
    fail_information <- data.frame(NULL, stringsAsFactors = FALSE)
    # campaign有錯重複查while條件
    fail_campaign_while <- 0
    # while最多重複幾次
    while_times <- 0
    # 彙整取得campaign的所有資料
    merge_data <- data.frame(NULL, stringsAsFactors = FALSE)


    # batch api設定
    fields <- "campaign_id,adset_id,ad_id,impressions,reach,clicks,spend,actions,action_values,unique_actions,conversions"
    # fields <- "campaign_id,adset_id,ad_id,impressions,reach,clicks,spend,actions,action_values,unique_actions"
    ad_level <- "ad"
    # breakdowns <- "['device_platform','publisher_platform']"
    breakdowns <- "[]"
    # filterings <- "[{'field':'ad.effective_status','operator':'IN','value':['ACTIVE','PAUSED','DELETED','PENDING_REVIEW','DISAPPROVED','PREAPPROVED','PENDING_BILLING_INFO','CAMPAIGN_PAUSED','ARCHIVED','ADSET_PAUSED','IN_PROCESS','WITH_ISSUES']},{'field':'impressions','operator':'GREATER_THAN','value':0}]"
    filterings <- "[{'field':'ad.effective_status','operator':'IN','value':['ACTIVE','PAUSED','DELETED','PENDING_REVIEW','DISAPPROVED','PREAPPROVED','PENDING_BILLING_INFO','CAMPAIGN_PAUSED','ARCHIVED','ADSET_PAUSED','IN_PROCESS','WITH_ISSUES']}]"
    time_range <- sprintf("{'since':'%s','until':'%s'}", start_date, end_date)
    time_increment <- 1 # 一天為資料單位
    limit_nrow <- 1000
    attribution_windows <- "['28d_click','7d_click','1d_view','1d_click']"
    # attribution_windows <- "['7d_click','1d_view']"


    # i <- 1
    for (i in 1:length(run_app_no)){
      # flog.info(paste(exe_datetime, "facebook_daily_insight start:", run_app_no[i], Sys.time(), collapse = " "), name = project_name)
      print(paste(exe_datetime, "facebook_daily_data start:", run_app_no[i], Sys.time(), collapse = " "))

      # access_token
      access_token <- switch(run_app_no[i],
                             mobile_app = mobile_access_token,
                             web_app = web_access_token,
                             NA)

      # 使用者的廣告帳戶
      ad_campaign <- campaign_list %>% filter(app_no == run_app_no[i]) %>% select(campaign_id) %>% unique()

      # 如果有失敗的campaign資訊，會再跑一次迴圈特別撈這些campaign資訊
      while (fail_campaign_while == 0 & while_times <= 10) {

        #### =========================================================================================== ####
        ####  一開始fail_campaign_id設空的df，若出現失敗的campaign會根據while迴圈繼續跑失敗的ad_campaign ####
        #### =========================================================================================== ####

        # 第一次先跑全部campaign，如果有失敗campaign只跑失敗的
        if (nrow(fail_campaign_id) == 0) {
          ad_campaign <- ad_campaign
        } else {
          ad_campaign <- fail_campaign_id %>% distinct(campaign_id)
        }

        # ===== 1. batch api 取得該帳戶所有廣告成本 =====
        if (nrow(ad_campaign) > 0){

          # ==== 要跑幾次batch，預設50個campaign一組 ====
          # j <- 1
          batch_request_time <- seq(from = 1, to = nrow(ad_campaign) , by = fb_max_request)
          for (j in 1:length(batch_request_time)){
            Sys.sleep(10)
            # flog.info(paste(exe_datetime, "facebook_daily_insight POST start:", batch_request_time[j], run_app_no[i], Sys.time(), collapse = " "), name = project_name)
            print(paste(exe_datetime, "facebook_daily_data POST start:", batch_request_time[j], run_app_no[i], Sys.time(), collapse = " "))

            # 每次batch資料暫存位置(最後合併至merge_data)
            batch_data <- data.frame(NULL, stringsAsFactors = FALSE)

            # batch campaign_id index
            campaign_range <- batch_request_time[j]:(ifelse((batch_request_time[j] + fb_max_request - 1) > nrow(ad_campaign), nrow(ad_campaign), batch_request_time[j] + fb_max_request - 1))

            # ==== 組合batch API Json格式 ====
            batch_campaign_id <- data.frame(campaign_id = ad_campaign$campaign_id[campaign_range], stringsAsFactors = FALSE)
            relative_url <- batch_campaign_id %>% apply(1, function(x){
              sprintf("%s/%s/insights?fields=%s&level=%s&breakdowns=%s&filtering=%s&time_range=%s&time_increment=%s&limit=%s&action_attribution_windows=%s", # &action_attribution_windows=
                      fb_api, x, fields, ad_level, breakdowns, filterings, time_range, time_increment, limit_nrow , attribution_windows)}) # attribution_windows

            batch_body <- data.frame(method = c("GET"), relative_url, stringsAsFactors = FALSE) %>%
              apply(1, function(x){ output <- sprintf('{"method":"%s","relative_url":"%s"}', x["method"], x["relative_url"])}) %>%
              paste0(., collapse = ",") %>% paste0("[", ., "]")

            # ==== 執行batch API ====
            Sys.sleep(5)
            tryCatch({
              batch_request <- POST(url = "https://graph.facebook.com",
                                    body = list(batch = batch_body, access_token = access_token),
                                    encode = "form",
                                    timeout(180))
            }, error = function(batch_timeout_error){
              # ===== 執行異常 1. batch api =====
              flog.info(paste(exe_datetime, "執行異常 1. batch api:", batch_request_time[j], run_app_no[i], Sys.time(), collapse = " "), name = project_name)
              print(paste(exe_datetime, "執行異常 1. batch api:", batch_request_time[j], run_app_no[i], Sys.time(), collapse = " "))

              # timeout例外處理
              # 紀錄失敗的campaign_id (因為error是function，所以要<<-放在全域變數才可被function外的使用)
              fail_batch_campaign_id <<- data.frame(app_no = run_app_no[i],
                                                    date = end_date,
                                                    campaign_id = batch_campaign_id$campaign_id ,
                                                    code = "daily_data",
                                                    status = 0,
                                                    update_time = exe_update_time,
                                                    stringsAsFactors = FALSE)
              fail_campaign_id <<- fail_campaign_id %>% rbind(fail_batch_campaign_id)

              #解析回傳錯誤
              error_message <- batch_timeout_error
              fail_information <<- fail_information %>% rbind(
                data.frame(error_message = sprintf("daily_insight batch api error - error_message : %s",
                                                   error_message),
                           stringsAsFactors = FALSE))
            })

            # 放在上面error = function 裡會失效，function裡的next跟for的環境不同
            if (exists("fail_batch_campaign_id")){
              if(sum(fail_batch_campaign_id$campaign_id %in% batch_campaign_id$campaign_id) > 0){
                # 結束這次j迴圈
                next
              }
            }

            flog.info(paste(exe_datetime, "facebook_daily_data POST:", run_app_no[i], Sys.time(), collapse = " "), name = project_name)
            print(paste(exe_datetime, "facebook_daily_data POST:", run_app_no[i], Sys.time(), collapse = " "))

            # ==== 確認這次batch是否成功，並把資料撈出 ====
            if(batch_request$status_code == 200){

                # campaign_id insert into API list
                batch_request_campaign_id <- batch_request %>% content
                for (k in 1:length(batch_request_campaign_id)){
                  batch_request_campaign_id[[k]]$campaign_id <- batch_campaign_id$campaign_id[k]
                }
                # 如果還傳資料沒有內容就不納入，大部分原因是當天沒有資料但是fb_insight_app_status卻顯示當天有資料
                batch_request_campaign_id[sapply(batch_request_campaign_id,function(x) x$body == '{\"data\":[]}')] <- NULL
                print( sapply(batch_request_campaign_id,function(x) x$body == '{\"data\":[]}') )


              batch_data <- batch_request_campaign_id %>%
                lapply(., function(x){

                  # 最後輸出資料
                  output <- data.frame(NULL, stringsAsFactors = FALSE)
                  # while停止設定
                  stop_while <- 0
                  # 執行分頁次數
                  paging_cnt <- 0

                  while (stop_while == 0){
                    if (paging_cnt == 0){
                      # flog.info(paste(exe_datetime, "facebook_daily_data batch first:", run_app_no[i], x$campaign_id, Sys.time(), collapse = " "), name = project_name)
                      paste(exe_datetime, "facebook_daily_data batch first:", run_app_no[i], x$campaign_id, Sys.time(), collapse = " ")

                            # 第一次batch api取得資料
                            if (x$code == 200){
                              json_data <- x$body %>% jsonlite::fromJSON(., flatten = TRUE) %>% `$`(data)
                              # # 測試：
                              # json_data <- batch_request_campaign_id[[1]]$body %>% jsonlite::fromJSON(., flatten = TRUE) %>% `$`(data)
                              # json_data$unique_actions
                              # json_data$actions[[5]]  %>% filter(action_type == "mobile_app_install")

                              # 測試某一個Campaign與Graph api差異(dafualt:7_d_click)
                              # test <- json_data$actions %>% lapply(., function(x){
                              #   TEST <- x %>% dplyr::filter(action_type == "mobile_app_install") %>%
                              #                 tidyr::spread(., action_type, value)
                              #   print(TEST)
                              # }
                              #   ) %>% do.call(plyr::rbind.fill, .)
                              # test$mobile_app_install %>% as.integer %>% sum()


                              if (!identical(json_data, list())){

                              jason_fn = function(df,jason_col,action_col){
                                         output <- df[[jason_col]] %>%
                                                      lapply(. , function(x){
                                                        # 如果Json欄位裡面出現NULL，需要轉換df才能rbind.fill
                                                          if(!(is.null(x))){
                                                              # action_json
                                                              is_empyt <- x %>% filter(action_type == action_col) %>% nrow
                                                              if (is_empyt == 0){
                                                                df <- data.frame(action_type = action_col , value = 0 , click_7d = 0 , view_1d = 0  , click_1d = 0, stringsAsFactors = FALSE)
                                                              } else{df <- x %>% dplyr::filter(action_type == action_col)}
                                                          } else{df <- data.frame(action_type = action_col , value = 0 , click_7d = 0 , view_1d = 0  , click_1d = 0, stringsAsFactors = FALSE)}
                                                          names(df)[names(df)=="28d_click"] <- "click_28d"
                                                          names(df)[names(df)=="7d_click"] <- "click_7d"
                                                          names(df)[names(df)=="1d_view"] <- "view_1d"
                                                          names(df)[names(df)=="1d_click"] <- "click_1d"
                                                          return(df)
                                                          }) %>% do.call(plyr::rbind.fill, .)
                                      return(output)
                              }
                              attribution_fn <- function(df,df_attribution){
                                  if(is.null(df[[df_attribution]])){
                                     attribution <- "0"} else{attribution <- df[[df_attribution]]}
                                  return(attribution)
                              }

                              # ===== batch conversions (優化事件數) =====
                              if (sum(names(json_data) == "conversions") == 1){
                                  conversions <- jason_fn(df = json_data , jason_col = 'conversions' , action_col = 'app_custom_event.four_hour' )[['value']]
                                  AEO_01 <- jason_fn(df = json_data , jason_col = 'conversions' , action_col = 'AEO_01' )[['value']]
                                  AEO_02 <- jason_fn(df = json_data , jason_col = 'conversions' , action_col = 'AEO_02' )[['value']]
                                  AEO_03 <- jason_fn(df = json_data , jason_col = 'conversions' , action_col = 'AEO_03' )[['value']]
                                  json_data$conversions <- conversions %>% as.character
                                  json_data$AEO_01 <- AEO_01 %>% as.character
                                  json_data$AEO_02 <- AEO_02 %>% as.character
                                  json_data$AEO_03 <- AEO_03 %>% as.character
                              } else{
                                json_data$conversions <- "0"
                                json_data$AEO_01 <- "0"
                                json_data$AEO_02 <- "0"
                                json_data$AEO_03 <- "0"
                                }
                              # ===== batch unique_actions (不重複儲值人數) =====
                              if (sum(names(json_data) == "unique_actions") == 1){
                                 # 1.unique_purchase
                                    unique_purchase <- jason_fn(df = json_data , jason_col = 'unique_actions' , action_col = 'app_custom_event.fb_mobile_purchase' )
                                    json_data$unique_purchase <- attribution_fn(df = unique_purchase , df_attribution = 'value') %>% as.character
                                    json_data$unique_purchase_28d_click <- attribution_fn(df = unique_purchase , df_attribution = 'click_28d') %>% as.character
                                    json_data$unique_purchase_7d_click <- attribution_fn(df = unique_purchase , df_attribution = 'click_7d') %>% as.character
                                    json_data$unique_purchase_1d_click <- attribution_fn(df = unique_purchase , df_attribution = 'click_1d') %>% as.character
                                    json_data$unique_purchase_1d_view <- attribution_fn(df = unique_purchase , df_attribution = 'view_1d') %>% as.character
                                  # json_data$unique_purchase <- unique_purchase %>% as.character # 若沒有轉成字串才會有不同格式的問題
                              } else{
                                json_data$unique_purchase <- "0"
                                json_data$unique_purchase_7d_click <- "0"
                                json_data$unique_purchase_1d_view <- "0"
                                json_data$unique_purchase_1d_click <- "0"}

                              # ===== batch 下載數 & lead ads & 粉絲團 成果 =====
                              if (sum(names(json_data) == "actions") == 1){
                                  # 1.mobile_app_install
                                    mobile_app_install <- jason_fn(df = json_data , jason_col = 'actions' , action_col = 'mobile_app_install' )
                                    json_data$app_install <- attribution_fn(df = mobile_app_install , df_attribution = 'value') %>% as.character
                                    json_data$app_install_28d_click <- attribution_fn(df = mobile_app_install , df_attribution = 'click_28d') %>% as.character
                                    json_data$app_install_7d_click <- attribution_fn(df = mobile_app_install , df_attribution = 'click_7d') %>% as.character
                                    json_data$app_install_1d_click <- attribution_fn(df = mobile_app_install , df_attribution = 'click_1d') %>% as.character
                                    json_data$app_install_1d_view <- attribution_fn(df = mobile_app_install , df_attribution = 'view_1d') %>% as.character
                                  # 2.onsite_conversion.lead_grouped
                                    leadgen <- jason_fn(df = json_data , jason_col = 'actions' , action_col = 'onsite_conversion.lead_grouped' )
                                    json_data$leadgen <- leadgen[['value']] %>% as.character
                                  # 3.like
                                    likes <- jason_fn(df = json_data , jason_col = 'actions' , action_col = 'like' )
                                    json_data$likes <- likes[['value']] %>% as.character
                              } else{
                                json_data$app_install <- "0"
                                json_data$app_install_7d_click <- "0"
                                json_data$app_install_1d_view <- "0"
                                json_data$app_install_1d_click <- "0"
                                json_data$leadgen <- "0"
                                json_data$likes <- "0"}

                              # ===== batch 儲值金額 =====
                              if (sum(names(json_data) == "action_values") == 1){
                                  fb_mobile_purchase <- jason_fn(df = json_data , jason_col = 'action_values' , action_col = 'app_custom_event.fb_mobile_purchase' )
                                  json_data$fb_mobile_purchase <- fb_mobile_purchase[['value']] %>% as.character
                              } else{json_data$fb_mobile_purchase <- "0"}


                              }
                              # 取得下頁url
                              # 測試next page data
                              # next_url <- batch_request_campaign_id[[1]]$body %>% jsonlite::fromJSON(., flatten = TRUE) %>% `$`(paging) %>% `$`(`next`)
                              next_url <- x$body %>% jsonlite::fromJSON(., flatten = TRUE) %>% `$`(paging) %>% `$`(`next`)
                              if (!is.null(next_url)){
                                # 如果有下一頁，就跑else(下一頁內容處理)
                                paging_cnt <- paging_cnt + 1
                              } else {
                                stop_while <- 1
                              }
                              # output <- rbind(output, json_data)
                              output <- rbind(output, json_data %>% select(., -c(starts_with("actions"),starts_with("action_values"),starts_with("unique_actions"))))
                              return(output)
                            } else {
                              # 紀錄失敗campaign_id
                              fail_json_campaign_id <- data.frame(app_no = run_app_no[i],
                                                                  date = end_date,
                                                                  campaign_id = x$campaign_id ,
                                                                  code = "daily_data",
                                                                  status = 0,
                                                                  update_time = exe_update_time,
                                                                  stringsAsFactors = FALSE)
                              fail_campaign_id <<- fail_campaign_id %>% rbind(fail_json_campaign_id)

                              # 解析回傳錯誤
                              fail_information <<- fail_information %>% rbind(
                                data.frame(error_message = sprintf("daily_insight error - campaign_id: %s  error_message : %s",
                                                                   x$campaign_id, x$body),
                                           stringsAsFactors = FALSE))

                              # 停止該campaign
                              stop_while <- 1
                            }

                    } else {
                      # flog.info(paste(exe_datetime, "facebook_daily_insight batch paging:", run_app_no[i], Sys.time(), collapse = " "), name = project_name)
                      paste(exe_datetime, "facebook_daily_insight batch paging:", run_app_no[i], Sys.time(), collapse = " ")

                      # 取得分頁資料
                      Sys.sleep(3)
                      tryCatch({
                        json_paging <- GET(next_url, timeout(180)) %>% content
                      }, error = function(paging_timeout_error){
                        # ===== 執行異常 2. batch paging =====
                        flog.info(paste(exe_datetime, "執行異常 2. batch paging:", batch_request_time[j], run_app_no[i], Sys.time(), collapse = " "), name = project_name)
                        print(paste(exe_datetime, "執行異常 2. batch paging:", batch_request_time[j], run_app_no[i], Sys.time(), collapse = " "))

                        # timeout例外處理
                        # 紀錄失敗的campaign_id
                        fail_paging_campaign_id <- data.frame(app_no = run_app_no[i],
                                                              date = end_date,
                                                              campaign_id = x$campaign_id ,
                                                              code = "daily_data",
                                                              status = 0,
                                                              update_time = exe_update_time,
                                                              stringsAsFactors = FALSE)
                        fail_campaign_id <<- fail_campaign_id %>% rbind(fail_paging_campaign_id)

                        #解析回傳錯誤
                        fail_information <<- fail_information %>% rbind(
                          data.frame(error_message = sprintf("daily_insight error - error_message : %s",
                                                             paging_timeout_error),
                                     stringsAsFactors = FALSE))

                        # 該次campagin_id停止
                        stop_while <- 1
                      })

                      # flog.info(paste(exe_datetime, "facebook_daily_insight GET:", batch_request_time[j], run_app_no[i], Sys.time(), collapse = " "), name = project_name)
                      # print(paste(exe_datetime, "facebook_daily_insight GET:", batch_request_time[j], run_app_no[i], Sys.time(), collapse = " "))

                      json_data <- json_paging$data %>% lapply(., function(x){

                                  #### **** list to df **** ####
                                  # 測試分頁數據：
                                  # page_df <- json_paging$data[[1]] %>% as.data.frame(stringsAsFactors = FALSE)  %>% select(., -c(starts_with("actions"),starts_with("action_values"),starts_with("unique_actions"),starts_with("conversions")))
                                  page_df <- x %>% as.data.frame(stringsAsFactors = FALSE) %>% select(., -c(starts_with("actions"),starts_with("action_values"),starts_with("unique_actions"),starts_with("conversions")))

                                  # ===== 分頁 優化事件數 =====
                                  if (!(is.null(x$conversions))){
                                        # conversions
                                        temp_conversions <- x$conversions %>% lapply(., as.data.frame, stringsAsFactors = FALSE) %>% do.call(plyr::rbind.fill, .)
                                          # conversions
                                          is_conversions <- temp_conversions %>% filter(action_type == "app_custom_event.four_hour") %>% nrow
                                          if (is_conversions != 0){
                                            conversions <- temp_conversions %>% filter(action_type == "app_custom_event.four_hour")
                                            page_df$conversions <- ifelse(is.null(conversions[['value']]),0,conversions[['value']])
                                            names(page_df)[names(page_df)=="app_custom_event.four_hour"] <- "conversions"
                                          } else{
                                            insert_fb_mobile_purchasel <- data.frame(conversions = 0, stringsAsFactors = FALSE)
                                            page_df %<>% cbind(insert_fb_mobile_purchasel)
                                          }
                                  } else{
                                    temp_conversions <- data.frame(conversions = 0 , stringsAsFactors = FALSE)
                                    page_df %<>% cbind(temp_conversions)
                                  }
                                  # ===== 分頁 不重複儲值人數 =====
                                  if (!(is.null(x$unique_actions))){
                                        # unique_actions
                                          temp_unique_actions <- x$unique_actions %>% lapply(., as.data.frame, stringsAsFactors = FALSE) %>% do.call(plyr::rbind.fill, .)
                                          # unique_actions
                                          is_unique_purchase <- temp_unique_actions %>% filter(action_type == "app_custom_event.fb_mobile_purchase") %>% nrow
                                          if (is_unique_purchase != 0){
                                            unique_purchase <- temp_unique_actions %>% filter(action_type == "app_custom_event.fb_mobile_purchase")
                                            page_df$unique_purchase <- ifelse(is.null(unique_purchase[['value']]),0,unique_purchase[['value']])
                                            names(page_df)[names(page_df)=="app_custom_event.fb_mobile_purchase"] <- "unique_purchase"
                                          } else{
                                            insert_fb_mobile_purchasel <- data.frame(unique_purchase = 0, stringsAsFactors = FALSE)
                                            page_df %<>% cbind(insert_fb_mobile_purchasel)
                                          }
                                  } else{
                                    temp_unique_purchase <- data.frame(unique_purchase = 0 , stringsAsFactors = FALSE)
                                    page_df %<>% cbind(temp_unique_purchase)
                                  }
                                  # ===== 分頁 下載數 =====
                                  if (!(is.null(x$actions))){
                                        # actions
                                        # temp_actions <- json_paging$data[[1]]$actions %>% lapply(., as.data.frame, stringsAsFactors = FALSE) %>% do.call(plyr::rbind.fill, .)
                                        temp_actions <- x$actions %>% lapply(., as.data.frame, stringsAsFactors = FALSE) %>% do.call(plyr::rbind.fill, .)
                                          # 一、安裝數
                                            is_install <- temp_actions %>% filter(action_type == "mobile_app_install") %>% nrow
                                            if (is_install != 0){
                                              app_install <- temp_actions %>% filter(action_type == "mobile_app_install")
                                              page_df$app_install <- ifelse(is.null(app_install[['value']]),0,app_install[['value']])
                                              page_df$app_install_7d_click <- ifelse(is.null(app_install[['X7d_click']]),0,app_install[['X7d_click']])
                                              page_df$app_install_1d_view  <- ifelse(is.null(app_install[['X1d_view']]),0,app_install[['X1d_view']])
                                              page_df$app_install_1d_click  <- ifelse(is.null(app_install[['X1d_click']]),0,app_install[['X1d_click']])
                                              names(page_df)[names(page_df)=="mobile_app_install"] <- "app_install"
                                            } else{
                                              insert_install <- data.frame(app_install = 0 , app_install_7d_click = 0 , app_install_1d_view = 0, stringsAsFactors = FALSE)
                                              page_df %<>% cbind(insert_install)
                                            }
                                          # 二、lead ads 廣告成果
                                            is_leadgen <- temp_actions %>% filter(action_type == "onsite_conversion.lead_grouped") %>% nrow
                                            if (is_leadgen != 0){
                                              leadgen <- temp_actions %>% filter(action_type == "onsite_conversion.lead_grouped")
                                              page_df$leadgen <- ifelse(is.null(leadgen[['value']]),0,leadgen[['value']])
                                              names(page_df)[names(page_df)=="onsite_conversion.lead_grouped"] <- "leadgen"
                                            } else{
                                              insert_leadgen <- data.frame(leadgen = 0 , stringsAsFactors = FALSE)
                                              page_df %<>% cbind(insert_leadgen)
                                            }
                                          # 三、粉絲團 廣告成果
                                            is_likes <- temp_actions %>% filter(action_type == "likes") %>% nrow
                                            if (is_likes != 0){
                                              likes <- temp_actions %>% filter(action_type == "likes")
                                              page_df$likes <- ifelse(is.null(likes[['value']]),0,likes[['value']])
                                            } else{
                                              insert_likes <- data.frame(likes = 0 , stringsAsFactors = FALSE)
                                              page_df %<>% cbind(insert_likes)
                                            }
                                  } else{
                                    temp_actions <- data.frame(app_install = 0 , app_install_7d_click = 0 , app_install_1d_view = 0 , app_install_1d_click = 0, stringsAsFactors = FALSE)
                                    page_df %<>% cbind(temp_actions)
                                  }
                                  # ===== 分頁 儲值金額 =====
                                  if (!(is.null(x$action_values))){
                                        # action_values
                                        temp_action_values <- x$action_values %>% lapply(., as.data.frame, stringsAsFactors = FALSE) %>% do.call(plyr::rbind.fill, .)
                                          # fb_mobile_purchase
                                          is_fb_mobile_purchase <- temp_action_values %>% filter(action_type == "app_custom_event.fb_mobile_purchase") %>% nrow
                                          if (is_fb_mobile_purchase != 0){
                                            fb_mobile_purchase <- temp_action_values %>% filter(action_type == "app_custom_event.fb_mobile_purchase")
                                            page_df$fb_mobile_purchase <- ifelse(is.null(fb_mobile_purchase[['value']]),0,fb_mobile_purchase[['value']])
                                            names(page_df)[names(page_df)=="app_custom_event.fb_mobile_purchase"] <- "fb_mobile_purchase"
                                          } else{
                                            insert_fb_mobile_purchasel <- data.frame(fb_mobile_purchase = 0, stringsAsFactors = FALSE)
                                            page_df %<>% cbind(insert_fb_mobile_purchasel)
                                          }
                                  } else{
                                    temp_actions <- data.frame(fb_mobile_purchase = 0 , stringsAsFactors = FALSE)
                                    page_df %<>% cbind(temp_actions)
                                  }
                                  return(page_df)
                                  }) %>% do.call(plyr::rbind.fill, .)

                      # 取得下次分頁url
                      next_url <- json_paging$paging$`next`
                      if (!is.null(next_url)){
                        paging_cnt <- paging_cnt + 1
                      } else {
                        stop_while <- 1
                      }
                      output <- rbind(output, json_data)
                    }
                  }
                  return(output)
                }) %>% do.call(bind_rows, .)

            } else {

              #####################################################################################
              ##########  註：整個batch_request 接資料失敗錯誤就紀錄：fail_campaign_id。 ##########
              #####################################################################################

              # ===== 執行異常 3. web_status =====
              # flog.info(paste(exe_datetime, "執行異常 3. web_status:", batch_request_time[j], run_app_no[i], Sys.time(), collapse = " "), name = project_name)
              # print(paste(exe_datetime, "執行異常 3. web_status:", batch_request_time[j], run_app_no[i], Sys.time(), collapse = " "))

              # batch api執行錯誤
              # 紀錄失敗campaign_id
              fail_batch_campaign_id <- data.frame(app_no = run_app_no[i],
                                                   date = end_date,
                                                   campaign_id = batch_campaign_id$campaign_id ,
                                                   code = "daily_data",
                                                   status = 0,
                                                   update_time = exe_update_time,
                                                   stringsAsFactors = FALSE)
              fail_campaign_id <<- fail_campaign_id %>% rbind(fail_batch_campaign_id)

              # 解析回傳錯誤
              batch_request_content <- batch_request %>% content
              error_code <- batch_request_content$error$code
              error_message <- batch_request_content$error$message
              fail_information <<- fail_information %>% rbind(
                data.frame(error_message = sprintf("daily_insight error - web_status : %s  error_code : %s  error_message : %s",
                                                   batch_request$status_code, error_code, error_message),
                           stringsAsFactors = FALSE))
            }

            # ==== 紀錄成功的campaign資料，加總每次batch的結果 ====
            if (exists("batch_data")){
              if (nrow(batch_data) > 0){
                # 紀錄有成功的campaign_id
                temp_success_campaign_id <- batch_data %>% select(campaign_id) %>% distinct %>% mutate(app_no = run_app_no[i], date = end_date, code = "daily_data", status = 1, update_time = exe_update_time)
                success_campaign_id <- success_campaign_id %>% rbind(temp_success_campaign_id)

                # 沒有spend欄位要補
                if (sum(names(batch_data) == "spend") == 0){
                  batch_data$spend <- "0"
                }

                # 把每次batch的資料加總起來
                batch_data %<>% select(-date_start) %>%
                  plyr::rename(c("date_stop" = "date", "fb_mobile_purchase" = "purchase")) %>%
                  filter(impressions != 0 | reach != 0 | clicks != 0 | spend != 0 | app_install != 0 | leadgen != 0 | likes != 0 | purchase != 0)
                merge_data %<>% rbind(., batch_data)
              }
            }
          }

          # ==== 有失敗的campaign資料重跑回圈 ====
          # 如果失敗的campaign重新撈取後有成功即排除
          if (nrow(fail_campaign_id) > 0) {
            fail_campaign_id %<>% anti_join(success_campaign_id, by = 'campaign_id') %>% distinct()
          }

          if (nrow(fail_campaign_id) > 0) {
            # 繼續while迴圈，並記錄次數
            fail_campaign_while <- 0
            while_times <- while_times+1
            # 重製錯誤訊息
            fail_information <- data.frame(NULL, stringsAsFactors = FALSE)
            print(sprintf("facebook_daily_data 有%s個失敗的campaign，第%s次重新要求資料", nrow(fail_campaign_id), while_times))
          } else {
            # 停止while迴圈
            fail_campaign_while <- 1
            print(sprintf("facebook_daily_data 沒有失敗的campaign", nrow(fail_campaign_id)))
          }
        } else {
          # 沒有campaign資料，停止while迴圈
          fail_campaign_while <- 1
          print("沒有campaign 資訊")
        }
      }
    }

    # ===== json finish =====
    flog.info(paste(exe_datetime, "json finish:", Sys.time(), collapse = " "), name = project_name)
    print(paste(exe_datetime, "json finish:", Sys.time(), collapse = " "))

    # ===== 2. 將API資料整合 =====
    if (nrow(merge_data) > 0){

              ############################################################################################
              ##########  註：此段主要是與mobile_game_fb_campaign Join，或取channel、group_id。 ##########
              ############################################################################################

      merge_data %<>% tidyr::replace_na(list(purchase = 0))
      merge_data %<>% tidyr::replace_na(list(unique_purchase = 0))

      flog.info(paste(exe_datetime, "API資料整合:", Sys.time(), collapse = " "), name = project_name)
      print(paste(exe_datetime, "API資料整合:", Sys.time(), collapse = " "))

      # 強制將af_channel變小寫
      # merge_data$af_channel <- merge_data$af_channel %>% tolower

      # 取得手遊group_id, channel
      fb_mobile_SQL <- "SELECT group_id, channel, campaign_id, adset_id, ad_id FROM mobile_game_fb_campaign;"
      fb_mobile_data <- dbGetQuery(connect_DB, fb_mobile_SQL)

      # # 取得頁遊gameid, promotecode
      # # gameid = 61 (攻城掠地聯運:攻城G妹)
      # fb_web_SQL <- "SELECT gameid AS group_id, promotecode, campaign_id, adset_id, ad_id FROM web_game_fb_campaign where gameid != 61;"
      # fb_web_data <- dbGetQuery(connect_DB, fb_web_SQL)


              #############################################################
              ##########  註：計算數據將資料整併成想要的樣樣子。 ##########
              #############################################################


      # 將資料整合並彙總(有些資料af_channel、ad_id會相同)
      mobile_game_fb_daily_data <- fb_mobile_data %>% inner_join(merge_data) %>%
        # select(-c(account_id, account_name, device_platform)) %>%
        # select(-c(device_platform)) %>%
        group_by(group_id , channel, campaign_id, adset_id, ad_id, date) %>%
        dplyr::summarise(impressions = sum(as.numeric(impressions)),
                         reach       = sum(as.numeric(reach)),
                         clicks      = sum(as.numeric(clicks)),
                         spend       = sum(as.numeric(spend)),
                         app_install = sum(as.numeric(app_install)),
                         app_install_28d_click = sum(as.numeric(app_install_28d_click)),
                         app_install_7d_click = sum(as.numeric(app_install_7d_click)),
                         app_install_1d_view = sum(as.numeric(app_install_1d_view)),
                         app_install_1d_click = sum(as.numeric(app_install_1d_click)),
                         conversions = sum(as.numeric(conversions)),
                         AEO_01 = sum(as.numeric(AEO_01)),
                         AEO_02 = sum(as.numeric(AEO_02)),
                         AEO_03 = sum(as.numeric(AEO_03)),
                         dpu = sum(as.numeric(unique_purchase)),
                         dpu_28d_click = sum(as.numeric(unique_purchase_28d_click)),
                         dpu_7d_click = sum(as.numeric(unique_purchase_7d_click)),
                         dpu_1d_view = sum(as.numeric(unique_purchase_1d_click)),
                         dpu_1d_click = sum(as.numeric(unique_purchase_1d_view)),
                         purchase    = sum(as.numeric(purchase)),
                         leadgen     = sum(as.numeric(leadgen)),
                         likes       = sum(as.numeric(likes))) %>%
        ungroup()

      # fb_web_daily_insight <- fb_web_data %>% inner_join(merge_data) %>%
      #   # select(-c(account_id, account_name, device_platform, af_channel)) %>% plyr::rename(c("group_id" = "gameid")) %>%
      #   # select(-c(device_platform)) %>%
      #   plyr::rename(c("group_id" = "gameid")) %>%
      #   group_by(gameid, promotecode, campaign_id, adset_id, ad_id, date) %>%
      #   dplyr::summarise(impressions = sum(as.numeric(impressions)),
      #                    reach       = sum(as.numeric(reach)),
      #                    clicks      = sum(as.numeric(clicks)),
      #                    spend       = sum(as.numeric(spend)),
      #                    app_install = sum(as.numeric(app_install)),
      #                    dpu = sum(as.numeric(unique_purchase)),
      #                    purchase    = sum(as.numeric(purchase)),
      #                    leadgen     = sum(as.numeric(leadgen)),
      #                    likes       = sum(as.numeric(likes))) %>%
      #   ungroup()


              #############################################################
              ##########   註：mobile_game_fb_daily_data 寫入db    ##########
              #############################################################

      # # 手遊資料寫入
      # if (nrow(mobile_game_fb_daily_data) > 0){
      #
      #   # 將資料寫入(SQL版本)
      #   install_values <- mobile_game_fb_daily_data %>%
      #     apply(., 1, function(x){
      #       output <- x %>%
      #         paste0(., collapse = "', '") %>%
      #         paste0("('", ., "')")
      #
      #       return(output) }) %>%
      #     paste0(., collapse = ",")
      #
      #   # mobile_game_fb_daily_data 寫入DB
      #   install_SQL <- sprintf("INSERT mobile_game_fb_daily_data (%s) VALUES ",
      #                          paste0(names(mobile_game_fb_daily_data), collapse = ", "))
      #
      #   insert_install_SQL <- paste0(install_SQL, install_values, " ON DUPLICATE KEY UPDATE channel = VALUES(channel), impressions = VALUES(impressions), reach = VALUES(reach), clicks = VALUES(clicks), app_install = VALUES(app_install), spend = VALUEs(spend), purchase = VALUES(purchase), leadgen = VALUES(leadgen), likes = VALUES(likes) , conversions = VALUES(conversions) , dpu = VALUES(dpu);")
      #   dbSendQuery(connect_DB, insert_install_SQL)
      # }


      # 設定每次匯入筆數
      parser_n <- 10000
      # 寫入DB mobile_game_fb_daily_data
      print("Start Insert mobile_game_fb_daily_data")
      if (nrow(mobile_game_fb_daily_data) > 0){

        for (i in seq(1, nrow(mobile_game_fb_daily_data), by = parser_n)){

          start_ind <- i
          end_ind <- i - 1 + parser_n

          if (end_ind > nrow(mobile_game_fb_daily_data)){
            end_ind <- nrow(mobile_game_fb_daily_data)
          }
          print(i)
          print(Sys.time())
          # 將資料寫入(SQL版本)
          insert_values <- mobile_game_fb_daily_data[start_ind:end_ind,] %>% apply(1, NAtoNULL) %>% paste0(., collapse = ",")
          insert_SQL <- sprintf("INSERT mobile_game_fb_daily_data (%s) VALUES ",
                                paste0(names(mobile_game_fb_daily_data), collapse = ", "))

          # ON DUPLICATE KEY UPDATE 組字串
          DUPLICATE_KEY_UPDATE_SQL <- names(mobile_game_fb_daily_data) %>% paste0(" = VALUES(",.,")") %>%
            paste0(names(mobile_game_fb_daily_data),.) %>%
            paste0(collapse = " , ") %>%
            paste0(" ON DUPLICATE KEY UPDATE ",.,";")
          insert_mobile_game_fb_daily_data_SQL <- paste0(insert_SQL, insert_values, DUPLICATE_KEY_UPDATE_SQL)
          dbSendQuery(connect_DB, insert_mobile_game_fb_daily_data_SQL)
        }
      }


              #############################################################
              ##########     註：fb_web_daily_insight 寫入db     ##########
              #############################################################

      # # 頁遊資料寫入
      # if (nrow(fb_web_daily_insight) > 0){
      #
      #   # 將資料寫入(SQL版本)
      #   install_values <- fb_web_daily_insight %>%
      #     apply(., 1, function(x){
      #       output <- x %>%
      #         paste0(., collapse = "', '") %>%
      #         paste0("('", ., "')")
      #
      #       return(output) }) %>%
      #     paste0(., collapse = ",")
      #
      #   # web_game_fb_daily_insight 寫入DB
      #   install_SQL <- sprintf("INSERT web_game_fb_daily_insight (%s) VALUES ",
      #                          paste0(names(fb_web_daily_insight), collapse = ", "))
      #
      #   insert_install_SQL <- paste0(install_SQL, install_values, " ON DUPLICATE KEY UPDATE promotecode = VALUES(promotecode), impressions = VALUES(impressions), reach = VALUES(reach), clicks = VALUES(clicks), app_install = VALUES(app_install), spend = VALUEs(spend), purchase = VALUES(purchase), leadgen = VALUES(leadgen), likes = VALUES(likes), leadgen = VALUES(leadgen), likes = VALUES(likes);")
      #   dbSendQuery(connect_DB, insert_install_SQL)
      # }
    }

    # ===== DB寫入完成 =====
    flog.info(paste(exe_datetime, "DB寫入完成:", Sys.time(), collapse = " "), name = project_name)
    print(paste(exe_datetime, "DB寫入完成:", Sys.time(), collapse = " "))

    # ===== 3. 更新成功與失敗的campaign_id狀態 =====
    # 成功campaign_id寫入fb_insight_app_status
    if (nrow(success_campaign_id) > 0){
      # 將資料寫入(SQL版本)
      success_campaign_values <- success_campaign_id %>%
        apply(., 1, function(x){
          output <- x %>%
            paste0(., collapse = "', '") %>%
            paste0("('", ., "')")

          return(output) }) %>%
        paste0(., collapse = ",")

      success_campaign_SQL <- sprintf("INSERT fb_insight_app_status (%s) VALUES ",
                                      paste0(names(success_campaign_id), collapse = ", "))

      insert_success_campaign_SQL <- paste0(success_campaign_SQL, success_campaign_values, " ON DUPLICATE KEY UPDATE status = VALUES(status), update_time = VALUES(update_time);")
      dbSendQuery(connect_DB, insert_success_campaign_SQL)
    }

    # 失敗的campaign_id寫入fb_insight_app_status
    if (nrow(fail_campaign_id) > 0){
      # 將資料寫入(SQL版本)
      fail_campaign_values <- fail_campaign_id %>%
        apply(., 1, function(x){
          output <- x %>%
            paste0(., collapse = "', '") %>%
            paste0("('", ., "')")

          return(output) }) %>%
        paste0(., collapse = ",")

      fail_campaign_SQL <- sprintf("INSERT fb_insight_app_status (%s) VALUES ",
                                   paste0(names(fail_campaign_id), collapse = ", "))

      insert_fail_campaign_SQL <- paste0(fail_campaign_SQL, fail_campaign_values, " ON DUPLICATE KEY UPDATE status = VALUES(status), update_time = VALUES(update_time);")
      dbSendQuery(connect_DB, insert_fail_campaign_SQL)
    }

    # 寄fail_information
    if (nrow(fail_information) > 0){
      special_data <- fail_information %>% select(error_message) %>%
        apply(., 1, paste0, collapse = "~~") %>% paste0(., collapse = "\n\n")

      send.mail(from = mysql_data$mail$from,
                to = mysql_data$mail$to,
                subject = sprintf("facebook_daily_data API錯誤訊息 - %s", exe_datetime),
                body = sprintf("錯誤訊息：\n FB API執行失敗\n %s\n資料未全部更新完成，請務必注意資料有無缺失",
                               special_data),
                encoding = "utf-8",
                smtp = list(host.name = "aspmx.l.google.com", port = 25),
                authenticate = FALSE,
                send = TRUE)
    }
  } else{
    flog.info(paste0(exe_datetime, " no data for facebook_daily_data ", end_date), name = project_name)
  }

  # 將程式結束時間寫入system_setting
  insert_sys_SQL <- sprintf("INSERT system_setting (category, name, value) VALUES ('FB_API_data_update', 'facebook_api_daily_data', '%s') ON DUPLICATE KEY UPDATE value=VALUES(value);",
                           Sys.time())

  dbSendQuery(connect_DB, insert_sys_SQL)

  # ===== Work End =====
  flog.info(paste0(exe_datetime, " finish facebook_daily_data ", end_date), name = project_name)

  if (exists("connect_DB")){
    dbDisconnect(connect_DB)
    rm(connect_DB)
  }

}, error = function(err){

  # campaign_list都算錯 寫入fb_insight_app_status
  if (nrow(campaign_list) > 0){
    all_fail <- campaign_list %>% mutate(code = "daily_data", status = 0, update_time = exe_update_time)
    # 將資料寫入(SQL版本)
    fail_campaign_values <- all_fail %>%
      apply(., 1, function(x){
        output <- x %>%
          paste0(., collapse = "', '") %>%
          paste0("('", ., "')")

        return(output) }) %>%
      paste0(., collapse = ",")

    fail_campaign_SQL <- sprintf("INSERT fb_insight_app_status (%s) VALUES ",
                                 paste0(names(all_fail), collapse = ", "))

    insert_fail_campaign_SQL <- paste0(fail_campaign_SQL, fail_campaign_values, " ON DUPLICATE KEY UPDATE status = VALUES(status), update_time = VALUES(update_time);")
    dbSendQuery(connect_DB, insert_fail_campaign_SQL)
  }

  # 寄信通知
  send.mail(from = mysql_data$mail$from,
            to = mysql_data$mail$to,
            subject = sprintf("facebook_daily_data 主程式錯誤訊息 - %s", exe_datetime),
            body = sprintf("錯誤訊息：\n 非預期程式執行失敗\n %s\n資料未全部更新完成，請務必注意資料有無缺失",
                           err),
            encoding = "utf-8",
            smtp = list(host.name = "aspmx.l.google.com", port = 25),
            authenticate = FALSE,
            send = TRUE)

  flog.error(paste0(exe_datetime, " Error Fail: ", err), name = project_name)

  # ===== 執行異常 4. 非預期錯誤 =====
  flog.info(paste(exe_datetime, "執行異常 4. 非預期錯誤", Sys.time(), collapse = " "), name = project_name)
  print(paste(exe_datetime, "執行異常 4. 非預期錯誤", Sys.time(), collapse = " "))

  if (exists("connect_DB")){
    dbDisconnect(connect_DB)
    rm(connect_DB)
  }

})
