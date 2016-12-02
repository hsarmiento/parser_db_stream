import sys

def concat_add_column(column):
	return "add column "+column

def concat_table_name(date,name):
	return date+name

def get_settings(filename):
	d = {'tables':[]}
	file = open(filename)
	i = 0
	for line in file:
		if i < 2:
			l = line.strip().split(":")
			d[l[0]] = l[1]
		else:
			d['tables'].append(line.strip())
		i+=1
	file.close()
	return d

def concat_schema(schema,tablename):
	return schema+'.'+tablename

settings = get_settings("tables_parser.txt")
tables_date = settings['tables']
schema_insert = settings['update']
schema_select = settings['source']
file = open("exec_source.sql","w")
file.write("-- REMOVE FOREIGN KEY RESTRICTION \n\n")
file.write("SELECT concat('alter table ',table_schema,'.',table_name,' DROP FOREIGN KEY ',constraint_name,';') FROM information_schema.table_constraints WHERE constraint_type='FOREIGN KEY' AND table_schema='"+settings['source']+"';\n\n")
file.write("set @@sql_mode='no_engine_substitution';\n\n")
file.write('-- ------------------------------------------------------- \n\n')

alter_table = "alter table {0} "
update_column_user = "update {0} set id_user = NULL;\n\n"

file.write('-- USERS TABLE------------------------------------------------------- \n\n\n')
tables = map(concat_table_name,tables_date,['_users']*len(tables_date))
add_columns_users = "sys_partition int(11) DEFAULT NULL, download_date timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, `name` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL, `blacklisted_user` tinyint(4) NOT NULL DEFAULT '0', `description` text CHARACTER SET utf8mb4 COLLATE utf8mb4_bin, `statuses_count` bigint(20) DEFAULT NULL, `favorites_count` bigint(20) DEFAULT NULL, `followers_count` bigint(20) DEFAULT NULL, `friends_count` bigint(20) DEFAULT NULL, `listed_count` bigint(20) DEFAULT NULL, `translator` tinyint(4) DEFAULT NULL, `geo_enabled` tinyint(4) DEFAULT NULL, `location` text CHARACTER SET utf8mb4 COLLATE utf8mb4_bin, `countries_user` varchar(200) DEFAULT NULL, `timezone` varchar(30) DEFAULT NULL, `follow_request_sent` tinyint(4) DEFAULT NULL, `uprotected` tinyint(4) DEFAULT NULL, `verified` tinyint(4) DEFAULT NULL, `contribution_enable` tinyint(4) DEFAULT NULL, `show_all_inline_media` tinyint(4) DEFAULT NULL, `url` text, `profile_bkg_color` varchar(6) DEFAULT NULL, `profile_bkg_img_url` text, `profile_img_url` text, `profile_link_color` varchar(6) DEFAULT NULL, `profile_sidebar_border_color` varchar(6) DEFAULT NULL, `profile_sidebar_color` varchar(6) DEFAULT NULL, `profile_text_color` varchar(6) DEFAULT NULL;"
add_columns_users = add_columns_users.split(", ")
add_columns_users = ', '.join(map(concat_add_column,add_columns_users))
change_columns_users = "change id_user id_user bigint(20) NOT NULL, change id_tweet id_tweet bigint(20) NOT NULL, change creation_date creation_date timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, change screen_name screen_name varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL, change id_lang_user lang_user varchar(10) DEFAULT NULL, change utc_offset utc_offset_mins int(11) DEFAULT NULL;"
update_columns_users = "update {0} set download_date = creation_date, sys_partition = 20, description = 'description', statuses_count = 0, favorites_count = 0, followers_count = 0, friends_count = 0, listed_count = 0, translator = 0, geo_enabled = 0, location = 'location', timezone = 'tz', follow_request_sent = 0, uprotected = 0, verified = 0, contribution_enable = 0, show_all_inline_media = 0, profile_bkg_color = 'C0DEED', profile_bkg_img_url = 'C0DEED', profile_img_url = 'C0DEED', profile_link_color = 'C0DEED', profile_sidebar_border_color = 'C0DEED', profile_sidebar_color = 'C0DEED', profile_text_color = 'C0DEED';"
for t in tables:
	file.write(alter_table.format(t)+add_columns_users+"\n\n")
	file.write(alter_table.format(t)+change_columns_users+'\n\n')
	file.write(update_columns_users.format(t)+"\n\n")
	file.write('-- ------------------------------------------------------- \n\n')


file.write('-- TWEETS TABLE------------------------------------------------------- \n\n\n')
tables = map(concat_table_name,tables_date,['_tweets']*len(tables_date))
add_columns_tweets = "`sys_partition` int(11) DEFAULT NULL, `download_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, `blacklisted_tweet` tinyint(4) NOT NULL DEFAULT '0', `repeated_user` tinyint(4) NOT NULL, `favorited` tinyint(4) NOT NULL, `truncated` tinyint(4) NOT NULL, `type` varchar(10) NOT NULL, `rt_count` bigint(20) DEFAULT NULL, `rt_id` bigint(20) DEFAULT NULL, `text_rt` text, `quote` tinyint(4) NOT NULL, `quote_id` bigint(20) DEFAULT NULL, `text_quote` text, `has_keyword` tinyint(4) NOT NULL DEFAULT '0', `src_href` varchar(200) DEFAULT NULL, `src_rel` varchar(50) DEFAULT NULL, `src_text` varchar(100) DEFAULT NULL, `in_reply_to_status_id` bigint(20) DEFAULT NULL, `in_reply_to_user_id` bigint(20) DEFAULT NULL, `in_reply_to_screen_name` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL, `countries_text` varchar(200) DEFAULT NULL, `geo_located` tinyint(4) NOT NULL, `geo_latitude` decimal(65,8) DEFAULT NULL, `geo_longitude` decimal(65,8) DEFAULT NULL, `place_id` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL, `place_name` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL, `place_country_code` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL, `place_country` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL, `place_fullname` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL, `place_street_address` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL, `place_url` varchar(2048) DEFAULT NULL, `place_place_type` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL, `counter` int(11) NOT NULL;"
add_columns_tweets = add_columns_tweets.split(", ")
add_columns_tweets = ', '.join(map(concat_add_column,add_columns_tweets))
change_columns_tweets = 'change creation_date creation_date timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,change id_tweet id_tweet bigint(20) NOT NULL,change text text_tweet text,change id_lang_text lang_tweet varchar(10) NOT NULL,change column id_user id_user bigint(20) NOT NULL,change rt rt tinyint(4) NOT NULL DEFAULT 0;'
update_column_from_user = "update {0} as t1 inner join {1}_users as t2 on t1.id_tweet = t2.id_tweet set t1.id_user = t2.id_user where t1.id_user is NULL; \n\n"
update_column_rt_true = "update {0} set rt = '1' where rt = 'true';\n\n" 
update_column_rt_false = "update {0} set rt = '0' where rt = 'false';\n\n" 
update_columns_after_join = "update {0} set id_user = 0 where id_user is NULL;\n\n"
update_columns_tweets = "update {0} set download_date = creation_date, sys_partition = 20, rt_count = 0, src_href = 'http://twitter.com', src_rel = 'nofollow', src_text = 'Twitter Web Client', type = 'TWEET';"
for t in tables:
	file.write(alter_table.format(t)+"change column id_user id_user varchar(30) DEFAULT NULL;\n\n")
	file.write(update_column_user.format(t))
	file.write(update_column_rt_true.format(t))
	file.write(update_column_rt_false.format(t))
	file.write(alter_table.format(t)+add_columns_tweets+"\n\n")
	file.write(alter_table.format(t)+change_columns_tweets+'\n\n')
	date,_ = t.split("_")
	file.write(update_column_from_user.format(t,date))
	file.write(update_columns_after_join.format(t))
	file.write(update_columns_tweets.format(t)+"\n\n")
	file.write('-- ------------------------------------------------------- \n\n')

file.write('-- HASHTAGS TABLE------------------------------------------------------- \n\n\n')
tables = map(concat_table_name,tables_date,['_e_hashtags']*len(tables_date))
add_columns_ht = "`sys_partition` int(11) DEFAULT NULL, download_date timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP;"
add_columns_ht = add_columns_ht.split(", ")
add_columns_ht = ', '.join(map(concat_add_column,add_columns_ht))
change_columns_ht = 'change id_tweet id_tweet bigint(20) NOT NULL,change hashtag hashtag text CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL, change start_idx idx_start int(11) NOT NULL, change end_idx idx_end int(11) NOT NULL;'
update_columns_ht = "update {0} set sys_partition = 20;"
for t in tables:
	file.write(alter_table.format(t)+add_columns_ht+"\n\n")
	file.write(alter_table.format(t)+change_columns_ht+'\n')
	file.write(update_columns_ht.format(t)+"\n\n")
	file.write('-- ------------------------------------------------------- \n\n')


file.write('-- e_urefs TABLE------------------------------------------------------- \n\n\n')
tables = map(concat_table_name,tables_date,['_e_urefs']*len(tables_date))
add_columns_urefs = "`sys_partition` int(11) DEFAULT NULL, download_date timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, `user_name` varchar(400) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL;"
add_columns_urefs = add_columns_urefs.split(", ")
add_columns_urefs = ', '.join(map(concat_add_column,add_columns_urefs))
change_columns_urefs = "change id_user id_user bigint(20) NOT NULL, change id_tweet id_tweet bigint(20) NOT NULL, change start_idx idx_start int(11) NOT NULL, change end_idx idx_end int(11) NOT NULL, change user_at `user_screen_name` varchar(400) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL;"
update_columns_urefs = "update {0} set sys_partition = 20, user_name = user_screen_name;"
for t in tables:
	file.write(alter_table.format(t)+add_columns_urefs+"\n\n")
	file.write(alter_table.format(t)+change_columns_urefs+'\n\n')
	file.write(update_columns_urefs.format(t)+"\n\n")
	file.write('-- ------------------------------------------------------- \n\n')


file.write('-- e_urls TABLE------------------------------------------------------- \n\n\n')
tables = map(concat_table_name,tables_date,['_e_urls']*len(tables_date))
add_columns_urls = "`sys_partition` int(11) DEFAULT NULL, download_date timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP;"
add_columns_urls = add_columns_urls.split(", ")
add_columns_urls = ', '.join(map(concat_add_column,add_columns_urls))
change_columns_urls = "change id_tweet id_tweet bigint(20) NOT NULL, change start_idx idx_start int(11) NOT NULL, change end_idx idx_end int(11) NOT NULL, change url_short url_short text NOT NULL, change url_full url_long text NOT NULL;"
update_columns_urls = "update {0} set sys_partition = 20;"
for t in tables:
	file.write(alter_table.format(t)+add_columns_urls+"\n\n")
	file.write(alter_table.format(t)+change_columns_urls+'\n')
	file.write(update_columns_urls.format(t)+"\n\n")
	file.write('-- ------------------------------------------------------- \n\n')

file.write('-- e_medias TABLE------------------------------------------------------- \n\n\n')
tables = map(concat_table_name,tables_date,['_e_medias']*len(tables_date))
create_table = "DROP TABLE IF EXISTS `{0}`; /*!40101 SET @saved_cs_client = @@character_set_client */; /*!40101 SET character_set_client = utf8 */; CREATE TABLE `{0}` ( `download_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, `id_tweet` bigint(20) NOT NULL, `id_media` bigint(20) NOT NULL, `url` text NOT NULL, `url_https` text NOT NULL, `type` varchar(50) NOT NULL, `sizes` text NOT NULL, `sys_partition` int(11) DEFAULT NULL, PRIMARY KEY (`id_tweet`,`id_media`), KEY `download_date` (`download_date`) USING BTREE ) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4; /*!40101 SET character_set_client = @saved_cs_client */;"
for t in tables:
	file.write(create_table.format(t)+"\n\n")
	file.write('-- ------------------------------------------------------- \n\n')
file.close()
file = open("exec_updated.sql","w")
file.write('-- CREATE AND INSERT INTO NEW SCHEMA------------------------------------------------------- \n\n\n')
create_tables = "/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */; /*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */; /*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */; /*!40101 SET NAMES utf8 */; /*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */; /*!40103 SET TIME_ZONE='+00:00' */; /*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */; /*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */; /*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */; /*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */; DROP TABLE IF EXISTS `{0}_e_hashtags`; /*!40101 SET @saved_cs_client = @@character_set_client */; /*!40101 SET character_set_client = utf8 */; CREATE TABLE `{0}_e_hashtags` ( `download_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, `id_tweet` bigint(20) NOT NULL, `idx_start` int(11) NOT NULL, `idx_end` int(11) NOT NULL, `hashtag` text CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL, `sys_partition` int(11) DEFAULT NULL, PRIMARY KEY (`id_tweet`,`idx_start`,`idx_end`), KEY `download_date` (`download_date`) USING BTREE ) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4; /*!40101 SET character_set_client = @saved_cs_client */; DROP TABLE IF EXISTS `{0}_e_medias`; /*!40101 SET @saved_cs_client = @@character_set_client */; /*!40101 SET character_set_client = utf8 */; CREATE TABLE `{0}_e_medias` ( `download_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, `id_tweet` bigint(20) NOT NULL, `id_media` bigint(20) NOT NULL, `url` text NOT NULL, `url_https` text NOT NULL, `type` varchar(50) NOT NULL, `sizes` text NOT NULL, `sys_partition` int(11) DEFAULT NULL, PRIMARY KEY (`id_tweet`,`id_media`), KEY `download_date` (`download_date`) USING BTREE ) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4; /*!40101 SET character_set_client = @saved_cs_client */; DROP TABLE IF EXISTS `{0}_e_urefs`; /*!40101 SET @saved_cs_client = @@character_set_client */; /*!40101 SET character_set_client = utf8 */; CREATE TABLE `{0}_e_urefs` ( `download_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, `id_tweet` bigint(20) NOT NULL, `idx_start` int(11) NOT NULL, `idx_end` int(11) NOT NULL, `id_user` bigint(20) NOT NULL, `user_name` varchar(400) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL, `user_screen_name` varchar(400) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL, `sys_partition` int(11) DEFAULT NULL, PRIMARY KEY (`id_tweet`,`idx_start`,`idx_end`), KEY `download_date` (`download_date`) USING BTREE ) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4; /*!40101 SET character_set_client = @saved_cs_client */; DROP TABLE IF EXISTS `{0}_e_urls`; /*!40101 SET @saved_cs_client = @@character_set_client */; /*!40101 SET character_set_client = utf8 */; CREATE TABLE `{0}_e_urls` ( `download_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, `id_tweet` bigint(20) NOT NULL, `idx_start` int(11) NOT NULL, `idx_end` int(11) NOT NULL, `url_short` text NOT NULL, `url_long` text NOT NULL, `sys_partition` int(11) DEFAULT NULL, PRIMARY KEY (`id_tweet`,`idx_start`,`idx_end`), KEY `download_date` (`download_date`) USING BTREE ) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4; /*!40101 SET character_set_client = @saved_cs_client */; DROP TABLE IF EXISTS `{0}_tweets`; /*!40101 SET @saved_cs_client = @@character_set_client */; /*!40101 SET character_set_client = utf8 */; CREATE TABLE `{0}_tweets` ( `download_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, `creation_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, `id_tweet` bigint(20) NOT NULL, `id_user` bigint(20) NOT NULL, `favorited` tinyint(4) NOT NULL, `truncated` tinyint(4) NOT NULL, `lang_tweet` varchar(10) NOT NULL, `text_tweet` text, `rt` tinyint(4) NOT NULL, `rt_count` bigint(20) DEFAULT NULL, `rt_id` bigint(20) DEFAULT NULL, `text_rt` text, `quote` tinyint(4) NOT NULL, `quote_id` bigint(20) DEFAULT NULL, `text_quote` text, `src_href` varchar(200) DEFAULT NULL, `src_rel` varchar(50) DEFAULT NULL, `src_text` varchar(100) DEFAULT NULL, `in_reply_to_status_id` bigint(20) DEFAULT NULL, `in_reply_to_user_id` bigint(20) DEFAULT NULL, `in_reply_to_screen_name` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL, `geo_located` tinyint(4) NOT NULL, `geo_latitude` decimal(65,8) DEFAULT NULL, `geo_longitude` decimal(65,8) DEFAULT NULL, `place_id` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL, `place_name` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL, `place_country_code` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL, `place_country` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL, `place_fullname` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL, `place_street_address` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL, `place_url` varchar(2048) DEFAULT NULL, `place_place_type` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL, `counter` int(11) NOT NULL, `type` varchar(10) NOT NULL, `countries_text` varchar(200) DEFAULT NULL, `repeated_user` tinyint(4) NOT NULL, `sys_partition` int(11) DEFAULT NULL, `blacklisted_tweet` tinyint(4) NOT NULL DEFAULT '0', `has_keyword` tinyint(4) NOT NULL DEFAULT '0', PRIMARY KEY (`id_tweet`), KEY `creation_date` (`creation_date`) USING BTREE, KEY `download_date` (`download_date`) USING BTREE ) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4; /*!40101 SET character_set_client = @saved_cs_client */; DROP TABLE IF EXISTS `{0}_users`; /*!40101 SET @saved_cs_client = @@character_set_client */; /*!40101 SET character_set_client = utf8 */; CREATE TABLE `{0}_users` ( `download_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, `creation_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, `id_tweet` bigint(20) NOT NULL, `id_user` bigint(20) NOT NULL, `name` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL, `screen_name` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL, `description` text CHARACTER SET utf8mb4 COLLATE utf8mb4_bin, `statuses_count` bigint(20) DEFAULT NULL, `favorites_count` bigint(20) DEFAULT NULL, `followers_count` bigint(20) DEFAULT NULL, `friends_count` bigint(20) DEFAULT NULL, `listed_count` bigint(20) DEFAULT NULL, `translator` tinyint(4) DEFAULT NULL, `geo_enabled` tinyint(4) DEFAULT NULL, `lang_user` varchar(10) DEFAULT NULL, `location` text CHARACTER SET utf8mb4 COLLATE utf8mb4_bin, `timezone` varchar(30) DEFAULT NULL, `utc_offset_mins` int(11) DEFAULT NULL, `follow_request_sent` tinyint(4) DEFAULT NULL, `uprotected` tinyint(4) DEFAULT NULL, `verified` tinyint(4) DEFAULT NULL, `contribution_enable` tinyint(4) DEFAULT NULL, `show_all_inline_media` tinyint(4) DEFAULT NULL, `url` text, `profile_bkg_color` varchar(6) DEFAULT NULL, `profile_bkg_img_url` text, `profile_img_url` text, `profile_link_color` varchar(6) DEFAULT NULL, `profile_sidebar_border_color` varchar(6) DEFAULT NULL, `profile_sidebar_color` varchar(6) DEFAULT NULL, `profile_text_color` varchar(6) DEFAULT NULL, `countries_user` varchar(200) DEFAULT NULL, `blacklisted_user` tinyint(4) NOT NULL DEFAULT '0', `sys_partition` int(11) DEFAULT NULL, PRIMARY KEY (`id_tweet`,`id_user`) ) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4; /*!40101 SET character_set_client = @saved_cs_client */; /*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */; /*!40101 SET SQL_MODE=@OLD_SQL_MODE */; /*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */; /*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */; /*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */; /*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */; /*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */; /*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;"
insert_into = "insert into {1}.{0}_e_hashtags select download_date, id_tweet, idx_start, idx_end, hashtag, sys_partition from {2}.{0}_e_hashtags; insert into {1}.{0}_e_urefs select download_date, id_tweet, idx_start, idx_end, id_user, user_name, user_screen_name, sys_partition from {2}.{0}_e_urefs; insert into {1}.{0}_e_urls select download_date, id_tweet, idx_start, idx_end, url_short, url_long, sys_partition from {2}.{0}_e_urls; insert into {1}.{0}_users select download_date, creation_date, id_tweet, id_user, name, screen_name, description, statuses_count, favorites_count, followers_count, friends_count, listed_count, translator, geo_enabled, lang_user, location, timezone, utc_offset_mins, follow_request_sent, uprotected, verified, contribution_enable, show_all_inline_media, url, profile_bkg_color, profile_bkg_img_url, profile_img_url, profile_link_color, profile_sidebar_border_color, profile_sidebar_color, profile_text_color, countries_user, blacklisted_user, sys_partition from {2}.{0}_users; insert into {1}.{0}_tweets select download_date, creation_date, id_tweet, id_user, favorited, truncated, lang_tweet, text_tweet, rt, rt_count, rt_id, text_rt, quote, quote_id, text_quote, src_href, src_rel, src_text, in_reply_to_status_id, in_reply_to_user_id, in_reply_to_screen_name, geo_located, geo_latitude, geo_longitude, place_id, place_name, place_country_code, place_country, place_fullname, place_street_address, place_url, place_place_type, counter, type, countries_text, repeated_user, sys_partition, blacklisted_tweet, has_keyword from {2}.{0}_tweets;"
for t in tables_date:
	file.write(create_tables.format(t)+"\n\n")
	file.write(insert_into.format(t,schema_insert,schema_select)+"\n\n")
	file.write('-- ------------------------------------------------------- \n\n')
file.close()




