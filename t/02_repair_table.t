#!perl

## Test corrupting, then repairing, a test table

use 5.006;
use strict;
use warnings;
use Cwd;
use Test::Most;
use lib 't','.';
require 'pg_healer_test_setup.pl';
select(($|=1,select(STDERR),$|=1)[1]);

## Move all this to the shared file
my $psql   = $ENV{PG_PSQL} || 'psql';

my ($t,$msg);

recreate_db();

my $table = 'foobar';

run( "CREATE TABLE $table AS SELECT 12345::int AS id FROM generate_series(1,10)" );

bail_on_fail;

$t = 'Extension installs cleanly';
run( 'DROP EXTENSION IF EXISTS pg_healer' );
like( run( 'CREATE LANGUAGE plperlu'), qr/CREATE LANGUAGE/, "$t (plperlu)"); ## temporary!
like( run( 'CREATE EXTENSION pg_healer' ), qr/CREATE EXTENSION/, "$t (pg_healer)" );
run( 'CHECKPOINT' );

$t = '(freespace) Corrupting a page by adding junk to the free space in the middle';
$msg = run( qq{SELECT pg_healer_corrupt('$table', 'freespace')} );
like( $msg, qr/Free space corruption introduced/, $t );

$t = '(freespace) Selecting from the corrupted table gives expected error';
$msg = run( "SELECT id FROM $table LIMIT 1" );
like( $msg, qr/invalid page in block/, $t );
$t = '(freespace) Selecting from the corrupted table invokes auto healing';
like( $msg, qr/intrinsic healing/, $t );

$t = '(freespace) Selecting a second time from the corrupted table succeeds';
$msg = run( "SELECT id FROM $table LIMIT 1" );
like( $msg, qr/12345/, $t );

$t = '(lsn) Running pg_healer_cauldron works';
$msg = run( 'SELECT pg_healer_cauldron()' );
like( $msg, qr/pg_healer_cauldron/, $t );

$t = '(pd_special) Corrupting a page by changing the pd_special location';
$msg = run( qq{SELECT pg_healer_corrupt('$table', 'pd_special')} );
like( $msg, qr/pd_special/, $t );

$t = '(pd_special) Selecting from the corrupted table gives expected error';
$msg = run( "SELECT id FROM $table LIMIT 1" );

like( $msg, qr/ page verification failed/, $t );
$t = '(freespace) Selecting from the corrupted table invokes auto healing';
like( $msg, qr/external checksum/, $t );

$t = '(pd_special) Selecting a second time from the corrupted table succeeds';
$msg = run( "SELECT id FROM $table LIMIT 1" );
like( $msg, qr/12345/, $t );

$t = '(lsn) Corrupting a page by changing the LSN';
$msg = run( qq{SELECT pg_healer_corrupt('$table', 'pd_lsn')} );
like( $msg, qr/LSN/, $t );

$t = '(lsn) Selecting from the corrupted table gives expected error';
$msg = run( "SELECT id FROM $table LIMIT 1" );
like( $msg, qr/ page verification failed/, $t );
$t = '(freespace) Selecting from the corrupted table invokes auto healing';
like( $msg, qr/external checksum/, $t );

$t = '(lsn) Selecting a second time from the corrupted table succeeds';
$msg = run( "SELECT id FROM $table LIMIT 1" );
like( $msg, qr/12345/, $t );

## Modify the file, but do not rsync
$t = 'Modify the table but do not copy it';
$msg = run( "UPDATE $table SET id=id" );
like( $msg, qr/UPDATE/, $t );

$t = '(sizever) Corrupting a page by changing the pagesize header';
$msg = run( qq{SELECT pg_healer_corrupt('$table', 'pd_pagesize_version')} );
like( $msg, qr/pd_pagesize_version/, $t );

$t = '(sizever) Selecting from the corrupted table gives expected error';
$msg = run( "SELECT id FROM $table LIMIT 1" );

like( $msg, qr/ page verification failed/, $t );
$t = '(freespace) Selecting from the corrupted table invokes auto healing';
like( $msg, qr/intrinsic healing/, $t );

$t = '(sizever) Selecting a second time from the corrupted table succeeds';
$msg = run( "SELECT id FROM $table LIMIT 1" );
like( $msg, qr/12345/, $t );

$t = '(bad_checksum) Corrupting a page by changing the checksum';
$msg = rund( qq{SELECT pg_healer_corrupt('$table', 'checksum')} );
like( $msg, qr/Checksum forced/, $t );

$t = '(bad_checksum) Selecting from the corrupted table gives expected error';
$msg = rund( "SELECT id FROM $table LIMIT 1" );
like( $msg, qr/ page verification failed/, $t );
$t = '(bad_checksum) Selecting from the corrupted table invokes auto healing';
like( $msg, qr/external checksum/, $t );

$t = '(bad_checksum) Selecting a second time from the corrupted table succeeds';
$msg = run( "SELECT id FROM $table LIMIT 1" );
like( $msg, qr/12345/, $t );


done_testing();
