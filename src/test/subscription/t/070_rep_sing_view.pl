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
              CREATE TABLE tab2 (c int, d int);
              CREATE VIEW vista AS SELECT * FROM tab1;
              CREATE VIEW multi_vista AS SELECT * FROM tab1 UNION ALL SELECT * FROM tab2 UNION ALL SELECT * FROM vista;);
$node_publisher->safe_psql('postgres', $init);
$node_subscriber->safe_psql('postgres', $init);

my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
$node_publisher->safe_psql('postgres', 
    qq(CREATE PUBLICATION mypub FOR VIEW vista, multi_vista with (publish = 'insert');)
    );
$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION mysub CONNECTION '$publisher_connstr' PUBLICATION mypub;"
);

# Test INSERT DML on single view's dependency
$node_publisher->safe_psql(
	'postgres', qq(
		INSERT INTO vista VALUES (1, 2);
        INSERT INTO tab1 VALUES (3, 4);
));

$node_publisher->wait_for_catchup('mysub');

my $check_view_rows = qq(SELECT * from vista;);
my $check_multi_dep_view_rows = qq(SELECT * from multi_vista;);
my $list_views = qq(\\dv);
my $pub_result = $node_publisher->safe_psql('postgres', $check_view_rows);
my $sub_result = $node_subscriber->safe_psql('postgres', $check_view_rows);
is($pub_result, qq(1|2\n3|4), 'Sanity check');
is($sub_result, qq(1|2\n3|4), 'INSERT INTO single view replicated');

# Test INSERT DML on single view's multi-dependency
$node_publisher->safe_psql(
	'postgres', qq(
		INSERT INTO tab2 VALUES (5, 6);
));

$node_publisher->wait_for_catchup('mysub');

$pub_result = $node_publisher->safe_psql('postgres', $check_multi_dep_view_rows);
my $sub_multi_result = $node_subscriber->safe_psql('postgres', $check_multi_dep_view_rows);
is($pub_result, qq(1|2\n3|4\n5|6\n1|2\n3|4), 'Sanity check');
is($sub_multi_result, qq(1|2\n3|4\n5|6\n1|2\n3|4), 'INSERT INTO single view with multi dependency replicated');

# Test DELETE DML on single view's dependency
$node_publisher->safe_psql(
	'postgres', qq(
		DELETE FROM vista *;
));

$node_publisher->wait_for_catchup('mysub');

$pub_result = $node_publisher->safe_psql('postgres', $check_view_rows);
$sub_result = $node_subscriber->safe_psql('postgres', $check_view_rows);
$sub_multi_result = $node_subscriber->safe_psql('postgres', $check_multi_dep_view_rows);
is($pub_result, qq(), 'Sanity check');
is($sub_result, qq(1|2\n3|4), 'DELETE from single view correctly not replicated');

# Test vista DDL column (non-FORALLTABLES is not thoroughly supported by DDL replication)
# $node_publisher->safe_psql('postgres', qq(ALTER VIEW vista RENAME TO vista2));
# $pub_result = $node_publisher->safe_psql('postgres', $list_views);
# $sub_result = $node_subscriber->safe_psql('postgres', $list_views);
# is($pub_result, $sub_result, 'ALTER VIEW vista RENAME TO replicated successfully');

# Todo rename vista DDL column and others

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