#!perl

## Test corrupting, then repairing, a test table

use 5.006;
use strict;
use warnings;
use Cwd;
use Test::More;
use lib 't','.';
require 'pg_healer_test_setup.pl';
select(($|=1,select(STDERR),$|=1)[1]);

## Move all this to the shared file
my $datadir = 'testdata';
my $psql   = $ENV{PG_PSQL} || 'psql';
my $path = Cwd::abs_path();
my $socketdir = "$path/$datadir/socket";
-e $socketdir or mkdir $socketdir;
my $testport = '5578';

$psql .= " -h $socketdir -p $testport postgres";
my $SQL = 'DROP TABLE IF EXISTS foobar';
system "$psql -qc '$SQL'";
$SQL = 'CREATE TABLE foobar AS SELECT 12345::int AS id FROM generate_series(1,10)';
$SQL = 'CREATE TABLE foobar AS SELECT 12345::int AS id LIMIT 1';
system "$psql -qc '$SQL'";

my $t = 'Extension installs cleanly';
$SQL = q{DROP EXTENSION IF EXISTS pg_healer};
system qq{$psql -qc "$SQL"};

$SQL = q{CREATE EXTENSION pg_healer};
system qq{$psql -qc "$SQL"};
system qq{$psql -qc "CHECKPOINT"};

$t = '(freespace) Corrupting a page by adding junk to the free space in the middle';
$SQL = q{SELECT pg_healer_corrupt('foobar', 'freespace')};
#system qq{$psql -qc "checkpoint"};
system qq{$psql -qc "$SQL"};

#system qq{$psql -c "SELECT pg_relation_filepath('foobar')"};

$t = '(freespace) Selecting from the corrupted table gives expected error';
$SQL = q{SELECT id FROM foobar LIMIT 1};
my $magik = qx{ $psql -c "$SQL" 2>&1};
like ($magik, qr/ invalid page in block/, $t);

$t = '(freespace) Selecting a second time from the corrupted succeeds';
$magik = qx{ $psql -c "$SQL" 2>&1};
like ($magik, qr/12345/, $t);

$t = '(lsn) Running cauldron works';
$SQL = q{SELECT pg_healer_cauldron()};
system qq{$psql -qc "$SQL"};

$t = '(pd_special) Corrupting a page by changing the pd_special location';
$SQL = q{SELECT pg_healer_corrupt('foobar', 'pd_special')};
system qq{$psql -qc "$SQL"};

$t = '(pd_special) Selecting from the corrupted table gives expected error';
$SQL = q{SELECT id FROM foobar LIMIT 1};
$magik = qx{ $psql -c "$SQL" 2>&1};
like ($magik, qr/ page verification failed/, $t);

$t = '(pd_special) Selecting a second time from the corrupted table succeeds';
$magik = qx{ $psql -c "$SQL" 2>&1};
like ($magik, qr/12345/, $t);

$t = '(lsn) Corrupting a page by changing the LSN';
$SQL = q{SELECT pg_healer_corrupt('foobar', 'pd_lsn')};
system qq{$psql -qc "$SQL"};

$t = '(lsn) Selecting from the corrupted table gives expected error';
$SQL = q{SELECT id FROM foobar LIMIT 1};
$magik = qx{ $psql -c "$SQL" 2>&1};
like ($magik, qr/ page verification failed/, $t);

$t = '(lsn) Selecting a second time from the corrupted table succeeds';
$magik = qx{ $psql -c "$SQL" 2>&1};
like ($magik, qr/12345/, $t);

## Modify the file, but do not rsync
$SQL = "UPDATE foobar SET id=id";
system qq{$psql -c "$SQL"};

$t = '(sizever) Corrupting a page by changing the pagesize header';
$SQL = q{SELECT pg_healer_corrupt('foobar', 'pd_pagesize_version')};
system qq{$psql -qc "$SQL"};

$t = '(sizever) Selecting from the corrupted table gives expected error';
$SQL = q{SELECT id FROM foobar LIMIT 1};
$magik = qx{ $psql -c "$SQL" 2>&1};
like ($magik, qr/ page verification failed/, $t);

$t = '(sizever) Selecting a second time from the corrupted table succeeds';
$magik = qx{ $psql -c "$SQL" 2>&1};
like ($magik, qr/12345/, $t);

done_testing();
