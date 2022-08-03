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
);
$node_publisher->safe_psql('postgres', $init);
$node_subscriber->safe_psql('postgres', $init);

my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
$node_publisher->safe_psql('postgres', "CREATE PUBLICATION mypub FOR ALL TABLES;");

my $list_pubs = qq(SELECT * FROM pg_publication_tables);
my $pub_result = $node_publisher->safe_psql('postgres', $list_pubs);
is($pub_result, qq(mypub|public|tab1|{a,b}|), 'View publication created');

$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION mysub CONNECTION '$publisher_connstr' PUBLICATION mypub;"
);

#--Test CREATE DDL
# note insert into view can fail depending on rule/triggers. should have separate
# test case for that.
$node_publisher->safe_psql('postgres', qq(
	CREATE VIEW vista AS SELECT * FROM tab1;
	INSERT INTO vista VALUES (1, 2);
));
my $check_view_rows = qq(SELECT * from vista;);
$pub_result = $node_publisher->safe_psql('postgres', $list_pubs);
is($pub_result, qq(mypub|public|tab1|{a,b}|
mypub|public|vista|{a,b}|), 'View publication created');

$node_publisher->wait_for_catchup('mysub');

$pub_result = $node_publisher->safe_psql('postgres', $check_view_rows);
my $sub_result = $node_subscriber->safe_psql('postgres', $check_view_rows);
is($pub_result, qq(1|2), 'Sanity check');
is($sub_result, qq($pub_result), 'CREATE vista replicated successfully');

#--Test ALTER VIEW RENAME column DDL
$node_publisher->safe_psql('postgres', qq(ALTER VIEW vista RENAME b TO b2;));

$node_publisher->wait_for_catchup('mysub');

my $check_view_columns = qq(\\d vista);
$pub_result = $node_publisher->safe_psql('postgres', $check_view_columns);
$sub_result = $node_subscriber->safe_psql('postgres', $check_view_columns);
is($pub_result, qq(a|integer|||\nb2|integer|||), 'Sanity check');
is($sub_result, qq($pub_result), 'ALTER view RENAME column replicated successfully');

#--Test ALTER VIEW RENAME view DDL
$node_publisher->safe_psql('postgres', qq(ALTER VIEW vista RENAME TO vista2;));

$node_publisher->wait_for_catchup('mysub');

my $list_nonsystem_views = qq(SELECT table_name FROM INFORMATION_SCHEMA.views WHERE table_schema = ANY (current_schemas(false)););
$pub_result = $node_publisher->safe_psql('postgres', $list_nonsystem_views);
$sub_result = $node_subscriber->safe_psql('postgres', $list_nonsystem_views);
is($pub_result, qq(vista2), 'Sanity check');
is($sub_result, qq($pub_result), 'ALTER view RENAME TO replicated successfully');

#--Test DROP VIEW DDL
$node_publisher->safe_psql('postgres', qq(DROP VIEW vista2;));

$node_publisher->wait_for_catchup('mysub');

$pub_result = $node_publisher->safe_psql('postgres', $list_nonsystem_views);
$sub_result = $node_subscriber->safe_psql('postgres', $list_nonsystem_views);
is($pub_result, qq(), 'Sanity check');
is($sub_result, qq(), 'DROP view replicated successfully');

#--Test DROP VIEW CASCADE DDL
$node_publisher->safe_psql('postgres', qq(
	CREATE VIEW vista AS SELECT * FROM tab1;
	CREATE VIEW dep AS SELECT * FROM vista UNION ALL SELECT * FROM tab1;
));
$pub_result = $node_publisher->safe_psql('postgres', $list_nonsystem_views);
is($pub_result, qq(vista\ndep), 'Sanity check');

$node_publisher->safe_psql('postgres', qq(DROP VIEW vista CASCADE;));

$node_publisher->wait_for_catchup('mysub');

$pub_result = $node_publisher->safe_psql('postgres', $list_nonsystem_views);
$sub_result = $node_subscriber->safe_psql('postgres', $list_nonsystem_views);
is($pub_result, qq(), 'Sanity check');
is($sub_result, qq(), 'DROP view CASCADE replicated correctly');

pass "basic view replication DDL tests (for all tables) passed";

$node_subscriber->stop;
$node_publisher->stop;

done_testing();