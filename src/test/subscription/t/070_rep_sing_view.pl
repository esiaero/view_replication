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
			  CREATE TABLE tab3 (e int, f int);
              CREATE VIEW vista AS SELECT * FROM tab1;
              CREATE VIEW multi_vista AS SELECT * FROM tab1 UNION ALL SELECT * FROM tab2 UNION ALL SELECT * FROM vista;);
$node_publisher->safe_psql('postgres', $init);
$node_subscriber->safe_psql('postgres', $init);

my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
$node_publisher->safe_psql('postgres', 
    qq(CREATE PUBLICATION mypub FOR TABLE tab3, 
	   VIEW multi_vista with (publish = 'insert, delete');)
    );
$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION mysub CONNECTION '$publisher_connstr' PUBLICATION mypub;"
);

# Test publication creation
my $list_pubs = qq(SELECT * FROM pg_publication_tables);
my $pub_result = $node_publisher->safe_psql('postgres', $list_pubs);
is($pub_result, qq(mypub|public|tab1|{a,b}|
mypub|public|tab2|{c,d}|
mypub|public|tab3|{e,f}|
mypub|public|vista|{a,b}|
mypub|public|multi_vista|{a,b}|), 'View publication created');
#

# Test INSERT DML on single view's dependency
$node_publisher->safe_psql(
	'postgres', qq(
		INSERT INTO vista VALUES (1, 2);
        INSERT INTO tab1 VALUES (3, 4);
		INSERT INTO tab2 VALUES (5, 6);
));

$node_publisher->wait_for_catchup('mysub');

my $check_tab2_rows = qq(SELECT * from tab2;);
my $check_view_rows = qq(SELECT * from vista;);
my $list_views = qq(\\dv);
$pub_result = $node_publisher->safe_psql('postgres', $check_view_rows);
my $sub_result = $node_subscriber->safe_psql('postgres', $check_view_rows);
is($pub_result, qq(1|2\n3|4), 'Sanity check');
is($sub_result, qq(1|2\n3|4), 'INSERT INTO single view replicated');

my $check_multi_dep_view_rows = qq(SELECT * from multi_vista;);
$pub_result = $node_publisher->safe_psql('postgres', $check_multi_dep_view_rows);
is($pub_result, qq(1|2\n3|4\n5|6\n1|2\n3|4), 'Sanity check');
my $sub_multi_result = $node_subscriber->safe_psql('postgres', $check_multi_dep_view_rows);
my $pub_multi_result = $node_publisher->safe_psql('postgres', $check_multi_dep_view_rows);
is($pub_multi_result, qq(1|2\n3|4\n5|6\n1|2\n3|4), 'Sanity check');
is($sub_multi_result, qq(1|2\n3|4\n5|6\n1|2\n3|4), 'INSERT INTO single view with multi dependency replicated');
#

# Test ALTER PUBLICATION for single view
$pub_result = $node_publisher->safe_psql('postgres', $list_pubs);
is($pub_result, qq(mypub|public|tab1|{a,b}|
mypub|public|tab2|{c,d}|
mypub|public|tab3|{e,f}|
mypub|public|vista|{a,b}|
mypub|public|multi_vista|{a,b}|), 'Sanity check');

## Test ALTER PUBLICATION DROP quirks
$node_publisher->safe_psql('postgres', qq(
	ALTER PUBLICATION mypub DROP TABLE tab1;
	INSERT INTO vista VALUES (1, 2);
));

$node_publisher->wait_for_catchup('mysub');

$pub_result = $node_publisher->safe_psql('postgres', $check_view_rows);
$sub_result = $node_subscriber->safe_psql('postgres', $check_view_rows);
is($pub_result, qq(1|2\n3|4\n1|2), 'Sanity check');
is($sub_result, qq(1|2\n3|4), 'Dropped dependency prevents DML on view');


$node_publisher->safe_psql('postgres', qq(
	ALTER PUBLICATION mypub DROP VIEW multi_vista;
	INSERT INTO tab2 VALUES (10,11);
));
$pub_result = $node_publisher->safe_psql('postgres', $list_pubs);
is($pub_result, qq(mypub|public|tab2|{c,d}|
mypub|public|tab3|{e,f}|
mypub|public|vista|{a,b}|), 'ALTER PUBLICATION DROP single view correctly');

$node_publisher->wait_for_catchup('mysub');

$pub_result = $node_publisher->safe_psql('postgres', $check_tab2_rows);
$sub_result = $node_subscriber->safe_psql('postgres', $check_tab2_rows);
is($pub_result, qq(5|6\n10|11), 'Sanity check');
is($sub_result, qq(5|6\n10|11), 'Dropped view does not affect DML on dependency');


$node_publisher->safe_psql('postgres', qq(
	ALTER TABLE tab1 REPLICA IDENTITY FULL;
	ALTER PUBLICATION mypub DROP VIEW vista;
	DELETE FROM vista *;
));

$node_publisher->wait_for_catchup('mysub');

$pub_result = $node_publisher->safe_psql('postgres', $check_view_rows);
$sub_result = $node_subscriber->safe_psql('postgres', $check_view_rows);
$pub_multi_result = $node_publisher->safe_psql('postgres', $check_multi_dep_view_rows);
$sub_multi_result = $node_subscriber->safe_psql('postgres', $check_multi_dep_view_rows);
is($pub_result, qq(), 'Sanity check');
is($sub_result, qq(1|2\n3|4), 'DELETE FROM single view not replicated (dropped dependency + view)');
is($pub_multi_result, qq(5|6\n10|11), 'Sanity check');
is($sub_multi_result, qq(1|2\n3|4\n5|6\n10|11\n1|2\n3|4), 'Sanity check');
##

## Test ALTER PUBLICATION ADD VIEW quirks
$pub_result = $node_publisher->safe_psql('postgres', $list_pubs);
is($pub_result, qq(mypub|public|tab2|{c,d}|
mypub|public|tab3|{e,f}|), 'Sanity check');
$node_publisher->safe_psql('postgres', qq(
	ALTER PUBLICATION mypub DROP TABLE tab2, tab3;
	ALTER PUBLICATION mypub ADD VIEW vista;
));
$pub_result = $node_publisher->safe_psql('postgres', $list_pubs);
is($pub_result, qq(mypub|public|tab1|{a,b}|
mypub|public|vista|{a,b}|), 'ALTER PUBLICATION ADD VIEW correctly adds view + dependencies');
##

## Test ALTER PUBLICATION SET VIEW quirks
$node_publisher->safe_psql('postgres', qq(
	ALTER PUBLICATION mypub SET VIEW multi_vista;
));
$pub_result = $node_publisher->safe_psql('postgres', $list_pubs);
is($pub_result, qq(mypub|public|tab1|{a,b}|
mypub|public|tab2|{c,d}|
mypub|public|vista|{a,b}|
mypub|public|multi_vista|{a,b}|), 'ALTER PUBLICATION SET VIEW view correctly');
##

## Test ALTER PUBLICATION SET TABLE quirks
$node_publisher->safe_psql('postgres', qq(
	ALTER PUBLICATION mypub SET TABLE tab3;
));
$pub_result = $node_publisher->safe_psql('postgres', $list_pubs);
is($pub_result, qq(mypub|public|tab3|{e,f}|
mypub|public|vista|{a,b}|
mypub|public|multi_vista|{a,b}|), 'ALTER PUBLICATION SET TABLE does not remove views');
##

## Test ALTER PUBLICATION SET VIEW quirks
$node_publisher->safe_psql('postgres', qq(
	ALTER PUBLICATION mypub SET VIEW multi_vista;
));
$pub_result = $node_publisher->safe_psql('postgres', $list_pubs);
is($pub_result, qq(mypub|public|tab1|{a,b}|
mypub|public|tab2|{c,d}|
mypub|public|tab3|{e,f}|
mypub|public|vista|{a,b}|
mypub|public|multi_vista|{a,b}|), 'ALTER PUBLICATION SET VIEW for existing view adds dependencies');
##
#

# Test DROP VIEW effects
# Note that we set replica identity here: the same restriction
# on delete/update applies for tables applies for views as well.
$node_publisher->safe_psql('postgres', qq(
	ALTER PUBLICATION mypub DROP VIEW vista, multi_vista, TABLE tab1;
	ALTER TABLE tab1 REPLICA IDENTITY FULL;
	DELETE FROM vista *;
));
$pub_result = $node_publisher->safe_psql('postgres', $list_pubs);
is($pub_result, qq(mypub|public|tab2|{c,d}|
mypub|public|tab3|{e,f}|), 'Sanity check');

$node_publisher->wait_for_catchup('mysub');

$pub_result = $node_publisher->safe_psql('postgres', $check_view_rows);
$sub_result = $node_subscriber->safe_psql('postgres', $check_view_rows);
$sub_multi_result = $node_subscriber->safe_psql('postgres', $check_multi_dep_view_rows);
is($pub_result, qq(), 'Sanity check');
is($sub_result, qq(1|2\n3|4), 'DELETE FROM single view not replicated (ALTER dropped correctly)');
is($pub_multi_result, qq(5|6\n10|11), 'Sanity check');
is($sub_multi_result, qq(1|2\n3|4\n5|6\n10|11\n1|2\n3|4), 'DELETE FROM single view not replicated (ALTER dropped correctly)');

# Test vista DDL ALTER rename (non-FORALLTABLES is not thoroughly supported by DDL replication)
# $node_publisher->safe_psql('postgres', qq(ALTER VIEW vista RENAME TO vista2));
# $pub_result = $node_publisher->safe_psql('postgres', $list_views);
# $sub_result = $node_subscriber->safe_psql('postgres', $list_views);
# is($pub_result, $sub_result, 'ALTER VIEW vista RENAME TO replicated successfully');

# Todo rename vista DDL column and others

# TOdo drop vista DDL etc.

pass "DML replication for single views passed";

$node_subscriber->stop;
$node_publisher->stop;

done_testing();