# Copyright (c) 2022, PostgreSQL Global Development Group
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

my $create_table = qq(CREATE TABLE tab1 (a int, b int););
$node_publisher->safe_psql('postgres', $create_table);
$node_subscriber->safe_psql('postgres', $create_table);

my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
$node_publisher->safe_psql('postgres', "CREATE PUBLICATION mypub FOR ALL TABLES;");
$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION mysub CONNECTION '$publisher_connstr' PUBLICATION mypub;"
);

$node_publisher->safe_psql(
	'postgres', qq(
		INSERT INTO tab1 VALUES (1, 2);
		INSERT INTO tab1 VALUES (3, 4);
));

# -- CREATE MAT VIEW - should work from zheng's base ddl_rep
my $create_view = qq(CREATE MATERIALIZED VIEW vista AS SELECT * FROM tab1;);
$node_publisher->safe_psql('postgres', $create_view);

$node_publisher->wait_for_catchup('mysub');

my $check_view_rows = qq(SELECT * from vista;);
my $check_table_rows = qq(SELECT * from tab1;);

my $pub_result = $node_publisher->safe_psql('postgres', $check_view_rows);
my $sub_result = $node_subscriber->safe_psql('postgres', $check_view_rows);
is($sub_result, qq(1|2\n3|4), 'Sanity check');
is($sub_result, qq($pub_result), 'CREATE MATERIALIZED vista replicated successfully');

# -- INSERT pub ?
$node_publisher->safe_psql('postgres', qq(INSERT INTO tab1 VALUES (8, 9)));
$pub_result = $node_publisher->safe_psql('postgres', $check_table_rows);
$sub_result = $node_subscriber->safe_psql('postgres', $check_table_rows);
is($pub_result, qq(1|2\n3|4\n8|9), 'Sanity check');
is($sub_result, qq(1|2\n3|4\n8|9), 'Sanity check');

$pub_result = $node_publisher->safe_psql('postgres', $check_view_rows);
$sub_result = $node_subscriber->safe_psql('postgres', $check_view_rows);
is($pub_result, qq(1|2\n3|4), 'Sanity check');
is($sub_result, qq(1|2\n3|4), 'Sanity check');

$node_publisher->safe_psql('postgres', qq(REFRESH MATERIALIZED VIEW vista;));
$pub_result = $node_publisher->safe_psql('postgres', $check_view_rows);
$sub_result = $node_subscriber->safe_psql('postgres', $check_view_rows);
is($pub_result, qq(1|2\n3|4\n8|9), 'Sanity check');
is($sub_result, qq(1|2\n3|4\n8|9), 'REFRESH replicated');

done_testing();