# === Start ===
# 主程式 facebook_daily_data.R
# 維護者: Vincent Chang
# 更新日期: 2021-12-01
# === End ===



!/bin/bash

hours=$(date +"%k")
# echo $hours


  
# 1.error_exception (Nearly 30 days & 2 days)

if [ $hours -ge 1 ] & [ $hours -le 2 ];
then
  for i in $(seq 1 1 30)
  do
      end_days=`expr $i - 1`
      start_days=$i
      run_end_date=$(date -d "$end_days day ago" +"%Y-%m-%d")
      run_start_date=$(date -d "$start_days day ago" +"%Y-%m-%d")
      echo $i
      echo "$run_end_date"
      echo "$run_start_date"
      mode=error_exception
      # Rscript /root/R_project/R_work/facebook_daily/facebook_daily_data.R $mode $run_start_date $run_end_date
      # Rscript /home/poisson/Program_Schedule/2021-11-24_FB\ API/facebook_daily_data.R  $mode $run_start_date $run_end_date
      Rscript /home/poisson/Program_Schedule/2021-12-09_FB\ API/facebook_daily_data.R  $mode $run_start_date $run_end_date
      
  done
elif [  $hours -eq 17 ] || [ $hours -eq 18 ];
then
  # end_days=0
  # start_days=1
  # run_end_date=$(date -d "$end_days day ago" +"%Y-%m-%d")
  # run_start_date=$(date -d "$start_days day ago" +"%Y-%m-%d")
  # mode=error_exception
  # # Rscript /root/R_project/R_work/facebook_daily/facebook_daily_data.R $mode $run_start_date $run_end_date
  # Rscript /home/poisson/Program_Schedule/facebook_daily_data.R  $mode $run_start_date $run_end_date

  for i in $(seq 1 1 2)
  do
      end_days=`expr $i - 1`
      start_days=$i
      run_end_date=$(date -d "$end_days day ago" +"%Y-%m-%d")
      run_start_date=$(date -d "$start_days day ago" +"%Y-%m-%d")
      echo "$run_end_date"
      echo "$run_start_date"
      mode=error_exception
      # Rscript /root/R_project/R_work/facebook_daily/facebook_daily_data.R $mode $run_start_date $run_end_date
      # Rscript /home/poisson/Program_Schedule/2021-11-24_FB\ API/facebook_daily_data.R  $mode $run_start_date $run_end_date
      Rscript /home/poisson/Program_Schedule/2021-12-09_FB\ API/facebook_daily_data.R  $mode $run_start_date $run_end_date

  done
  
# 2.Normal Programing (Nearly 56 days)

elif [  $hours -ge 4 ] & [ $hours -le 5 ];
then
  for i in $(seq 7 8 56) # (FIRST,INCREMENT,LAST)
  do
      end_days=`expr $i - 7`
      start_days=$i
      run_end_date=$(date -d "$end_days day ago" +"%Y-%m-%d")
      run_start_date=$(date -d "$start_days day ago" +"%Y-%m-%d")
      echo "$i"
      echo "$run_end_date"
      echo "$run_start_date"
      mode=hour
      # Rscript /root/R_project/R_work/facebook_daily/facebook_daily_data.R $mode $run_start_date $run_end_date
      # Rscript /home/poisson/Program_Schedule/2021-11-24_FB\ API/facebook_daily_data.R  $mode $run_start_date $run_end_date
      Rscript /home/poisson/Program_Schedule/2021-12-09_FB\ API/facebook_daily_data.R  $mode $run_start_date $run_end_date
  done

else
    echo $hours
fi



  # 2021-11-29_補2021年的資料的數據
  
  # 設計迴圈(尚未解決)
  for i in $(seq 7 8 356)
  do  # 先print樹定時間
      # def_ini_date=$(date -d "120 day ago" +"%Y-%m-%d")
      # echo "$def_ini_date"

      end_days=`expr $i - 7`
      start_days=$i
      run_end_date=$(date -d "$end_days day ago" +"%Y-%m-%d")
      run_start_date=$(date -d "$start_days day ago" +"%Y-%m-%d")
      echo "$i"
      echo "$run_end_date"
      echo "$run_start_date"
      mode=hour
      # Rscript /root/R_project/R_work/facebook_daily/facebook_daily_data.R $mode $run_start_date $run_end_date
      # Rscript /home/poisson/Program_Schedule/2021-11-24_FB\ API/facebook_daily_data.R  $mode $run_start_date $run_end_date
      # Rscript /home/poisson/Program_Schedule/2021-12-09_FB\ API/facebook_daily_data.R  $mode $run_start_date $run_end_date

  done



# 2021-12-03 01:33:00 放在root試試看
# 6.FB_API數據：
# 10 * * * * Rscript /home/poisson/Program_Schedule/facebook_daily_data.R
# 30 * * * * Rscript /home/poisson/Program_Schedule/facebook_daily_data.R error_exception
# 30 * * * * sh /home/poisson/Program_Schedule/facebook_daily_data_fail_campaign.sh
# 10 * * * * Rscript /home/poisson/Program_Schedule/2021-11-24_FB\ API/facebook_daily_data.R










