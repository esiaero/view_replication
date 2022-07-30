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


my $init = qq(CREATE TABLE tab1 (a int, b int);
              CREATE VIEW vista AS SELECT * FROM tab1;);
$node_publisher->safe_psql('postgres', $init);
$node_subscriber->safe_psql('postgres', $init);

my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
$node_publisher->safe_psql('postgres', 
    "CREATE PUBLICATION mypub FOR VIEW vista with (publish = 'insert');"
#   "CREATE PUBLICATION mypub FOR VIEW wefefw with (publish = '');"
#     "CREATE PUBLICATION mypub FOR TABLE wefefw with (publish = '');"
    );
$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION mysub CONNECTION '$publisher_connstr' PUBLICATION mypub;"
);

#--Test INSERT DML for views
$node_publisher->safe_psql(
	'postgres', qq(
		INSERT INTO vista VALUES (1, 2);
));

$node_publisher->wait_for_catchup('mysub');

my $check_view_rows = qq(SELECT * from vista;);
my $pub_result = $node_publisher->safe_psql('postgres', $check_view_rows);
my $sub_result = $node_subscriber->safe_psql('postgres', $check_view_rows);
is($pub_result, qq(1|2), 'Sanity check');
is($sub_result, qq(1|2), 'INSERT INTO vista replicated successfully');

# Todo rename vista DDL

# #--Test DROP VIEW DDL, should work on ddl_replication
# my $drop_vista = qq(DROP VIEW vista2;);
# $node_publisher->safe_psql('postgres', $drop_vista);

# $node_publisher->wait_for_catchup('mysub');

# $pub_result = $node_publisher->safe_psql('postgres', $list_views);
# $sub_result = $node_subscriber->safe_psql('postgres', $list_views);
# is($sub_result, qq($pub_result), 'DROP VIEW vista replicated successfully');

pass "fill this out";

$node_subscriber->stop;
$node_publisher->stop;

done_testing();