package com.hortonworks.faas.spark.predictor.orm.service

import scalikejdbc._
import skinny.dbmigration.DBSeeds

trait CreateTables extends DBSeeds { self: Connection =>

  override val dbSeedsAutoSession = NamedAutoSession('service)

  addSeedSQL(
    sql"""
create table services (
  no bigint auto_increment primary key not null,
  name varchar(128) not null,
  created_at timestamp not null,
  updated_at timestamp not null,
  deleted_at timestamp
)
""",
    sql"""
create table applications (
  id bigint auto_increment primary key not null,
  name varchar(128) not null,
  service_no bigint not null references services(no),
  created_at timestamp not null,
  updated_at timestamp not null,
  deleted_at timestamp
)
""",
    sql"""
create table service_settings (
  id bigint auto_increment primary key not null,
  maximum_accounts bigint not null default 10000,
  service_no bigint not null references services(no),
  created_at timestamp not null,
  updated_at timestamp not null
)
""",
    sql"""
create table hanadb_active_object (
  id bigint auto_increment primary key not null,
  namespace VARCHAR(4000) not null,
  db_object_name VARCHAR(4000) not null,
  db_object_type VARCHAR(4000) not null,
  db_object_type_suffix VARCHAR(4000) not null,
  version bigint  default 1,
  activated_at timestamp,
  activated_by VARCHAR(4000),
  is_active BOOLEAN  default true,
  created_at timestamp not null,
  updated_at timestamp not null,
  deleted_at timestamp
)
""",
   sql"""
create table hanadb_active_object_detail (
  id bigint auto_increment primary key not null,
  haoid bigint not null references hanadb_active_object(id),
  column_name VARCHAR(4000) not null,
  is_key boolean  default false,
  col_order bigint,
  attribute_hierarchy_active boolean,
  display_attribute boolean default false,
  default_description VARCHAR(4000) not null,
  source_object_name VARCHAR(4000) not null,
  source_column_name VARCHAR(4000) not null,
  is_required_for_flow BOOLEAN  default true,
  created_at timestamp not null,
  updated_at timestamp not null,
  deleted_at timestamp
)
"""
  )

  runIfFailed(sql"select count(1) from services")
}
