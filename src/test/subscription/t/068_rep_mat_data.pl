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

$node_publisher->safe_psql('postgres', "CREATE PUBLICATION mypub2 FOR ALL TABLES with (publish = 'insert, refresh', refresh_data = true);");
$node_subscriber_2->safe_psql('postgres',
  "CREATE SUBSCRIPTION mysub2 CONNECTION '$publisher_connstr' PUBLICATION mypub2;"
);

# Test materialized view replication
$node_publisher->safe_psql('postgres', qq(CREATE TABLE tab1 (a int, b int);));
$node_publisher->wait_for_catchup('mysub');
$node_publisher->safe_psql('postgres', qq(
    CREATE MATERIALIZED VIEW vista AS SELECT * FROM tab1;));
$node_publisher->wait_for_catchup('mysub');


# Test materialized view data change replication
# If data change is NOT replicated (e.g., command replicated), publisher and subscriber will differ.
# If data change is replicated, running refresh should make NO changes on the publisher/subscriber vista.
$node_subscriber->safe_psql('postgres', qq(INSERT INTO tab1 VALUES (3, 4);));
$node_subscriber_2->safe_psql('postgres', qq(INSERT INTO tab1 VALUES (3, 4);));
$node_publisher->safe_psql('postgres', qq(INSERT INTO tab1 VALUES (1, 2);));
$node_publisher->safe_psql('postgres', qq(REFRESH MATERIALIZED VIEW vista;));

my $check_table_rows = qq(SELECT * from tab1;);
my $check_view_rows = qq(SELECT * from vista;);
my $pub_result = $node_publisher->safe_psql('postgres', $check_table_rows);
my $sub_result = $node_subscriber->safe_psql('postgres', $check_table_rows);
my $sub_2_result = $node_subscriber_2->safe_psql('postgres', $check_table_rows);
is($pub_result, qq(1|2), 'Sanity check'); 
is($sub_result, qq(3|4\n1|2), 'Sanity check');
is($sub_2_result, qq(3|4\n1|2), 'Sanity check');
$pub_result = $node_publisher->safe_psql('postgres', $check_view_rows);
$sub_result = $node_subscriber->safe_psql('postgres', $check_view_rows);
$sub_2_result = $node_subscriber_2->safe_psql('postgres', $check_view_rows);
is($pub_result, qq(1|2), 'Sanity check'); 
is($sub_result, qq(3|4\n1|2), 'REFRESH command was replicated');
# Refresh data change
is($sub_2_result, qq(1|2), 'REFRESH data command was replicated');


done_testing();