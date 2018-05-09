package com.hortonworks.faas.spark.predictor.orm.service

import scalikejdbc._
import skinny.dbmigration.DBSeeds

trait CreateTables extends DBSeeds { self: Connection =>

  override val dbSeedsAutoSession = NamedAutoSession('service)

//  addSeedSQL(
//    sql"""
//create table hanadb_active_object (
//  id bigint auto_increment primary key not null,
//  namespace VARCHAR(4000) not null,
//  db_object_name VARCHAR(4000) not null,
//  db_object_type VARCHAR(4000) not null,
//  db_object_type_suffix VARCHAR(4000) not null,
//  version bigint  default 1,
//  activated_at timestamp,
//  activated_by VARCHAR(4000),
//  is_active BOOLEAN  default true,
//  created_at timestamp not null DEFAULT CURRENT_TIMESTAMP,
//  updated_at timestamp not null DEFAULT CURRENT_TIMESTAMP,
//  deleted_at timestamp DEFAULT CURRENT_TIMESTAMP
//)
//""",
//   sql"""
//create table hanadb_active_object_detail (
//  id bigint auto_increment primary key not null,
//  haoid bigint not null references hanadb_active_object(id),
//  column_name VARCHAR(4000) not null,
//  is_key boolean  default false,
//  col_order bigint,
//  attribute_hierarchy_active boolean,
//  display_attribute boolean default false,
//  default_description VARCHAR(4000) not null,
//  source_object_name VARCHAR(4000) not null,
//  source_column_name VARCHAR(4000) not null,
//  is_required_for_flow BOOLEAN  default true,
//  created_at timestamp not null DEFAULT CURRENT_TIMESTAMP,
//  updated_at timestamp not null DEFAULT CURRENT_TIMESTAMP,
//  deleted_at timestamp DEFAULT CURRENT_TIMESTAMP
//)
//"""
//  )

  runIfFailed(sql"select count(1) from services")
}
