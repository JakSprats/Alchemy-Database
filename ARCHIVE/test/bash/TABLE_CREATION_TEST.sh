#!/bin/bash

CLI=./redisql-cli

$CLI CREATE TABLE \`exmpl5\` "( \
  \`id\` int(11) NOT NULL, \
  \`val\` text, \
  UNIQUE KEY \`id\` (\`id\`) \
) ENGINE=InnoDB DEFAULT CHARSET=latin1;"

$CLI CREATE TABLE \`exmpl6\` "( \
  \`id\` int(11) DEFAULT NULL, \
  \`blah\` text, \
  KEY \`id\` (\`id\`), \
  CONSTRAINT \`id_fkey\` FOREIGN KEY (\`id\`) REFERENCES \`exmpl5\` (\`id\`) ON DELETE NO ACTION \
) ENGINE=InnoDB DEFAULT CHARSET=latin1;"

$CLI CREATE TABLE \`product_order\` "( \
  \`no\` int(11) NOT NULL AUTO_INCREMENT, \
  \`product_category\` int(11) NOT NULL, \
  \`product_id\` int(11) NOT NULL, \
  \`customer_id\` int(11) NOT NULL, \
  PRIMARY KEY (\`no\`), \
  KEY \`product_category\` (\`product_category\`,\`product_id\`), \
  KEY \`customer_id\` (\`customer_id\`), \
  CONSTRAINT \`product_order_ibfk_1\` FOREIGN KEY (\`product_category\`, \`product_id\`) REFERENCES \`product\` (\`category\`, \`id\`) ON UPDATE CASCADE, \
  CONSTRAINT \`product_order_ibfk_2\` FOREIGN KEY (\`customer_id\`) REFERENCES \`customer\` (\`id\`) \
) ENGINE=InnoDB DEFAULT CHARSET=latin1;"

$CLI CREATE TABLE \`child\` "( \
  \`id\` int(11) DEFAULT NULL, \
  \`parent_id\` int(11) DEFAULT NULL, \
  KEY \`par_ind\` (\`parent_id\`), \
  CONSTRAINT \`child_ibfk_1\` FOREIGN KEY (\`parent_id\`) REFERENCES \`parent\` (\`id\`) ON DELETE CASCADE \
) ENGINE=InnoDB DEFAULT CHARSET=latin1;"

$CLI CREATE TABLE \`parent\` "( \
  \`id\` int(11) NOT NULL, \
  PRIMARY KEY (\`id\`) \
) ENGINE=InnoDB DEFAULT CHARSET=latin1;"

$CLI CREATE TABLE \`user\` "( \
  \`Host\` char(60) COLLATE utf8_bin NOT NULL DEFAULT '', \
  \`User\` char(16) COLLATE utf8_bin NOT NULL DEFAULT '', \
  \`Password\` char(41) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL DEFAULT ' ', \
  \`Select_priv\` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N', \
  \`Insert_priv\` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N', \
  \`Update_priv\` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N', \
  \`Delete_priv\` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N', \
  \`Create_priv\` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N', \
  \`Drop_priv\` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N', \
  \`Reload_priv\` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N', \
  \`Shutdown_priv\` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N', \
  \`Process_priv\` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N', \
  \`File_priv\` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N', \
  \`Grant_priv\` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N', \
  \`References_priv\` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N', \
  \`Index_priv\` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N', \
  \`Alter_priv\` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N', \
  \`Show_db_priv\` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N', \
  \`Super_priv\` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N', \
  \`Create_tmp_table_priv\` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N', \
  \`Lock_tables_priv\` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N', \
  \`Execute_priv\` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N', \
  \`Repl_slave_priv\` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N', \
  \`Repl_client_priv\` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N', \
  \`Create_view_priv\` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N', \
  \`Show_view_priv\` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N', \
  \`Create_routine_priv\` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N', \
  \`Alter_routine_priv\` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N', \
  \`Create_user_priv\` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N', \
  \`Event_priv\` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N', \
  \`Trigger_priv\` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N', \
  \`ssl_type\` enum('','ANY','X509','SPECIFIED') CHARACTER SET utf8 NOT NULL DEFAULT '', \
  \`ssl_cipher\` blob NOT NULL, \
  \`x509_issuer\` blob NOT NULL, \
  \`x509_subject\` blob NOT NULL, \
  \`max_questions\` int(11) unsigned NOT NULL DEFAULT '0', \
  \`max_updates\` int(11) unsigned NOT NULL DEFAULT '0', \
  \`max_connections\` int(11) unsigned NOT NULL DEFAULT '0', \
  \`max_user_connections\` int(11) unsigned NOT NULL DEFAULT '0', \
  PRIMARY KEY (\`Host\`,\`User\`) \
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='Users and global privileges';"

$CLI CREATE TABLE \`jos_content\` "( \
  \`id\` int(11) unsigned NOT NULL AUTO_INCREMENT, \
  \`title\` varchar(255) NOT NULL DEFAULT '', \
  \`alias\` varchar(255) NOT NULL DEFAULT '', \
  \`title_alias\` varchar(255) NOT NULL DEFAULT '', \
  \`introtext\` mediumtext NOT NULL, \
  \`fulltext\` mediumtext NOT NULL, \
  \`state\` tinyint(3) NOT NULL DEFAULT '0', \
  \`sectionid\` int(11) unsigned NOT NULL DEFAULT '0', \
  \`mask\` int(11) unsigned NOT NULL DEFAULT '0', \
  \`catid\` int(11) unsigned NOT NULL DEFAULT '0', \
  \`created\` datetime NOT NULL DEFAULT '0000-00-00 00:00:00', \
  \`created_by\` int(11) unsigned NOT NULL DEFAULT '0', \
  \`created_by_alias\` varchar(255) NOT NULL DEFAULT '', \
  \`modified\` datetime NOT NULL DEFAULT '0000-00-00 00:00:00', \
  \`modified_by\` int(11) unsigned NOT NULL DEFAULT '0', \
  \`checked_out\` int(11) unsigned NOT NULL DEFAULT '0', \
  \`checked_out_time\` datetime NOT NULL DEFAULT '0000-00-00 00:00:00', \
  \`publish_up\` datetime NOT NULL DEFAULT '0000-00-00 00:00:00', \
  \`publish_down\` datetime NOT NULL DEFAULT '0000-00-00 00:00:00', \
  \`images\` text NOT NULL, \
  \`urls\` text NOT NULL, \
  \`attribs\` text NOT NULL, \
  \`version\` int(11) unsigned NOT NULL DEFAULT '1', \
  \`parentid\` int(11) unsigned NOT NULL DEFAULT '0', \
  \`ordering\` int(11) NOT NULL DEFAULT '0', \
  \`metakey\` text NOT NULL, \
  \`metadesc\` text NOT NULL, \
  \`access\` int(11) unsigned NOT NULL DEFAULT '0', \
  \`hits\` int(11) unsigned NOT NULL DEFAULT '0', \
  \`metadata\` text NOT NULL, \
  PRIMARY KEY (\`id\`), \
  KEY \`idx_section\` (\`sectionid\`), \
  KEY \`idx_access\` (\`access\`), \
  KEY \`idx_checkout\` (\`checked_out\`), \
  KEY \`idx_state\` (\`state\`), \
  KEY \`idx_catid\` (\`catid\`), \
  KEY \`idx_createdby\` (\`created_by\`) \
) ENGINE=MyISAM AUTO_INCREMENT=47 DEFAULT CHARSET=utf8;"
