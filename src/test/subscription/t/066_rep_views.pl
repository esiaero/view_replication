# Copyright (c) 2022, PostgreSQL Global Development Group

# TO COMPLETE/TODO: Regression tests for logical replication of views
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# create publisher node
my $node_publisher = PostgreSQL::Test::Cluster->new('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->start;

# create subscriber node
my $node_subscriber = PostgreSQL::Test::Cluster->new('subscriber');
$node_subscriber->init(allows_streaming => 'logical');
$node_subscriber->append_conf('postgresql.conf', 'autovacuum = off');
$node_subscriber->start;

my $init = qq(
	CREATE TABLE tab1 (a int, b int);
	CREATE VIEW init_view AS SELECT * FROM tab1;
);
$node_publisher->safe_psql('postgres', $init);
$node_subscriber->safe_psql('postgres', $init);

my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
$node_publisher->safe_psql('postgres', "CREATE PUBLICATION mypub FOR ALL TABLES;");

my $list_pubs = qq(SELECT * FROM pg_publication_tables);
my $pub_result = $node_publisher->safe_psql('postgres', $list_pubs);
is($pub_result, qq(mypub|public|tab1|{a,b}|
mypub|public|init_view|{a,b}|), 'View publication created');

$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION mysub CONNECTION '$publisher_connstr' PUBLICATION mypub;"
);

#--Test CREATE DDL, should work on ddl_replication branch but not master
my $create_view = qq(CREATE VIEW vista AS SELECT * FROM tab1;);
$node_publisher->safe_psql('postgres', $create_view);

$node_publisher->wait_for_catchup('mysub');

my $check_view_rows = qq(SELECT * from vista;);
$pub_result = $node_publisher->safe_psql('postgres', $check_view_rows);
my $sub_result = $node_subscriber->safe_psql('postgres', $check_view_rows);
is($sub_result, qq($pub_result), 'CREATE vista replicated successfully');

#--INSERT INTO view DML test. This can fail for complicated by rules/triggers
my $insert_view = qq(INSERT INTO vista VALUES (8, 9););
$node_publisher->safe_psql('postgres', $insert_view);

$node_publisher->wait_for_catchup('mysub');

$pub_result = $node_publisher->safe_psql('postgres', $check_view_rows);
$sub_result = $node_subscriber->safe_psql('postgres', $check_view_rows);
is($sub_result, qq($pub_result), 'INSERT INTO view replicated successfully');





#--Test ALTER VIEW RENAME column DDL, should not work
my $alter_view = qq(ALTER VIEW vista RENAME b TO b2;);
$node_publisher->safe_psql('postgres', $alter_view);

$node_publisher->wait_for_catchup('mysub');

my $check_view_columns = qq(\\d vista);
$pub_result = $node_publisher->safe_psql('postgres', $check_view_columns);
$sub_result = $node_subscriber->safe_psql('postgres', $check_view_columns);
# is(0, qq(1), "there are $sub_result");
# is(0, qq(1), "there are $pub_result");
is($sub_result, qq($pub_result), 'ALTER view RENAME column replicated successfully');


# #--Test ALTER TABLE RENAME column DDL, should not work
my $alter_table = qq(ALTER TABLE tab1 RENAME b TO b2;);
$node_publisher->safe_psql('postgres', $alter_table);

$node_publisher->wait_for_catchup('mysub');

my $check_table_columns = qq(\\d tab1);
$pub_result = $node_publisher->safe_psql('postgres', $check_table_columns);
$sub_result = $node_subscriber->safe_psql('postgres', $check_table_columns);
is($sub_result, qq($pub_result), 'ALTER table RENAME column replicated successfully');





#--Test ALTER TABLE RENAME table DDL, should  work
my $rename_table = qq(ALTER TABLE tab1 RENAME TO tab2;);
$node_publisher->safe_psql('postgres', $rename_table);

$node_publisher->wait_for_catchup('mysub');

my $list_tables = qq(\\dt);
$pub_result = $node_publisher->safe_psql('postgres', $list_tables);
$sub_result = $node_subscriber->safe_psql('postgres', $list_tables);
is($sub_result, qq($pub_result), 'ALTER table RENAME tab1 replicated successfully');

#--Test ALTER VIEW RENAME view DDL, should work on ddl_replication
my $rename_view = qq(ALTER VIEW vista RENAME TO vista2;);
$node_publisher->safe_psql('postgres', $rename_view);

$node_publisher->wait_for_catchup('mysub');

my $list_views = qq(\\dv);
$pub_result = $node_publisher->safe_psql('postgres', $list_views);
$sub_result = $node_subscriber->safe_psql('postgres', $list_views);
is($sub_result, qq($pub_result), 'ALTER VIEW RENAME vista replicated successfully');

#--Test DROP VIEW DDL, should work on ddl_replication
my $drop_vista = qq(DROP VIEW vista2;);
$node_publisher->safe_psql('postgres', $drop_vista);

$node_publisher->wait_for_catchup('mysub');

$pub_result = $node_publisher->safe_psql('postgres', $list_views);
$sub_result = $node_subscriber->safe_psql('postgres', $list_views);
is($sub_result, qq($pub_result), 'DROP VIEW vista replicated successfully');

pass "basic view replication DDL tests (for all tables) passed";

$node_subscriber->stop;
$node_publisher->stop;

done_testing();