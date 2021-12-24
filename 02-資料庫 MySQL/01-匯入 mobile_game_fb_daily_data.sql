
USE analysis;
CREATE TABLE `mobile_game_fb_daily_data` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `group_id` varchar(60) NOT NULL COMMENT '遊戲id',
  -- `af_channel` varchar(50) NOT NULL COMMENT '版位',
  `channel` varchar(45) NOT NULL COMMENT '手機平台',
  `campaign_id` varchar(50) NOT NULL COMMENT '行銷活動id',
  `adset_id` varchar(50) NOT NULL COMMENT '廣告組合id',
  `ad_id` varchar(50) NOT NULL COMMENT '廣告id\n',
  `date` date NOT NULL COMMENT '廣告投放日期',
  `impressions` int(10) NOT NULL COMMENT '曝光數',
  `reach` int(10) NOT NULL DEFAULT '0' COMMENT '曝光數排除重複',
  `clicks` int(10) NOT NULL DEFAULT '0' COMMENT '點擊數',
  `app_install` int(10) NOT NULL DEFAULT '0' COMMENT '安裝數',
  `spend` float(10,2) NOT NULL DEFAULT '0.00' COMMENT '廣告費用',
  `dpu` int(10) NOT NULL DEFAULT '0' COMMENT '儲值人數 (原app_custom_event.fb_mobile_purchase欄位)',
  `purchase` float(10,2) NOT NULL DEFAULT '0.00' COMMENT '儲值金額',
  `leadgen` int(10) NOT NULL DEFAULT '0' COMMENT 'lead ads 成果',
  `likes` int(10) NOT NULL DEFAULT '0' COMMENT '粉絲團按讚 成果',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uni_key` (`group_id`,`campaign_id`,`adset_id`,`ad_id`,`date`)
) ENGINE=MyISAM AUTO_INCREMENT=1420309 DEFAULT CHARSET=utf8

conversions


-- 新增欄位  
USE analysis;
ALTER TABLE mobile_game_fb_daily_data
ADD `conversions` int(10) NOT NULL DEFAULT '0' COMMENT '事件優化數';


-- 2021-11-22 新增欄位  
USE analysis;
ALTER TABLE mobile_game_fb_daily_data
ADD `AEO_01` int(10) NOT NULL DEFAULT '0' COMMENT '事件優化數_01',
ADD `AEO_02` int(10) NOT NULL DEFAULT '0' COMMENT '事件優化數_02',
ADD `AEO_03` int(10) NOT NULL DEFAULT '0' COMMENT '事件優化數_03',
ADD `app_install_7d_click` int(10) NOT NULL DEFAULT '0' COMMENT '7天點擊後之安裝數',
ADD `app_install_1d_view` int(10) NOT NULL DEFAULT '0' COMMENT  '1天曝光之安裝數',
ADD `app_install_1d_click` int(10) NOT NULL DEFAULT '0' COMMENT '1天點擊後之安裝數';


-- 2021-12-09 刪除欄位 
USE analysis;
ALTER TABLE  mobile_game_fb_daily_data drop `app_install_7d_click`;
ALTER TABLE  mobile_game_fb_daily_data drop `app_install_1d_click`;
ALTER TABLE  mobile_game_fb_daily_data drop `app_install_1d_view`;


-- 2021-12-09 新增欄位 
USE analysis;
ALTER TABLE mobile_game_fb_daily_data
ADD `app_install_28d_click` int(10) NOT NULL DEFAULT '0' COMMENT '28天點擊後之安裝數',
ADD `app_install_7d_click` int(10) NOT NULL DEFAULT '0' COMMENT '7天點擊後之安裝數',
ADD `app_install_1d_click` int(10) NOT NULL DEFAULT '0' COMMENT '1天點擊後之安裝數',
ADD `app_install_1d_view` int(10) NOT NULL DEFAULT '0' COMMENT  '1天觀看後之安裝數',

ADD `dpu_28d_click` int(10) NOT NULL DEFAULT '0' COMMENT '28天點擊後之儲值人數',
ADD `dpu_7d_click` int(10) NOT NULL DEFAULT '0' COMMENT '7天點擊後之儲值人數',
ADD `dpu_1d_click` int(10) NOT NULL DEFAULT '0' COMMENT '1天點擊後之儲值人數',
ADD `dpu_1d_view` int(10) NOT NULL DEFAULT '0' COMMENT  '1天觀看後之儲值人數';
