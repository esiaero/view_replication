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

# create subscriber node 2
my $node_subscriber_2 = PostgreSQL::Test::Cluster->new('subscriber_2');
$node_subscriber_2->init(allows_streaming => 'logical');
$node_subscriber_2->append_conf('postgresql.conf', 'autovacuum = off');
$node_subscriber_2->start;

my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
$node_publisher->safe_psql('postgres', "CREATE PUBLICATION mypub FOR ALL TABLES with (publish = 'insert, refresh');");
$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION mysub CONNECTION '$publisher_connstr' PUBLICATION mypub;"
);

$node_publisher->safe_psql('postgres', "CREATE PUBLICATION mypub2 FOR ALL TABLES with (publish = 'insert');");
$node_subscriber_2->safe_psql('postgres',
	"CREATE SUBSCRIPTION mysub2 CONNECTION '$publisher_connstr' PUBLICATION mypub2;"
);

my $create_table = qq(CREATE TABLE tab1 (a int, b int);); # create ddl should be replicated
$node_publisher->safe_psql('postgres', $create_table);
$node_publisher->safe_psql('postgres', qq(INSERT INTO tab1 VALUES (1, 2);));

$node_publisher->safe_psql('postgres', qq(CREATE MATERIALIZED VIEW vista AS SELECT * FROM tab1;));
$node_publisher->safe_psql('postgres', qq(INSERT INTO tab1 VALUES (8, 9)));

$node_publisher->wait_for_catchup('mysub');
$node_publisher->wait_for_catchup('mysub2');

# -- REFRESH test
$node_publisher->safe_psql('postgres', qq(REFRESH MATERIALIZED VIEW vista;));

$node_publisher->wait_for_catchup('mysub');
$node_publisher->wait_for_catchup('mysub2');

my $check_view_rows = qq(SELECT * from vista;);
# my $check_table_rows = qq(SELECT * from tab1;);

my $pub_result = $node_publisher->safe_psql('postgres', $check_view_rows);
my $sub_result = $node_subscriber->safe_psql('postgres', $check_view_rows);
my $sub_result_2 = $node_subscriber_2->safe_psql('postgres', $check_view_rows);
is($pub_result, qq(1|2\n8|9), 'Sanity check');
is($sub_result, qq(1|2\n8|9), 'REFRESH replicated');
is($sub_result_2, qq(1|2), 'REFRESH should not have been replicated');

done_testing();