#!perl

## Start up a test cluster as needed

use 5.006;
use strict;
use warnings;
use autodie;
use Cwd;
use Test::More;
use lib 't','.';
require 'pg_healer_test_setup.pl';
select(($|=1,select(STDERR),$|=1)[1]);

my $datadir = 'testdata';

my $initdb = $ENV{PG_INITDB} || 'initdb';
my $pgctl  = $ENV{PG_PGCTL}  || 'pg_ctl';
my $psql   = $ENV{PG_PSQL}   || 'psql';

if (! -e $datadir) {
    system "$initdb --data-checksums $datadir";
}

my $path = Cwd::abs_path();
my $socketdir = "$path/$datadir/socket";
-e $socketdir or mkdir $socketdir;

my $testport = '5578';

my $conf = "$datadir/postgresql.conf";
open my $fh, '+<', $conf;
my %found;
while (<$fh>) {
    if (/pg_healer/) {
        $found{extension} = 1;
    }
    if (/$socketdir/) {
        $found{socketdir} = 1;
    }
    if (/port/ and /$testport/) {
        $found{port} = 1;
    }
}
if (! exists $found{extension}) {
    seek $fh, 0, 2;
    print {$fh} "\nshared_preload_libraries = 'pg_healer'\n\n";
}
if (! exists $found{socketdir}) {
    seek $fh, 0, 2;
    print {$fh} "\nunix_socket_directories = '$socketdir'\n\n";
}
if (! exists $found{port}) {
    seek $fh, 0, 2;
    print {$fh} "\nport = $testport\n\n";
    print {$fh} "\nshared_buffers = 128kB\n\n";
}

close $fh;

my $COM;
my $pidfile = "$datadir/postmaster.pid";
if (-e $pidfile) {
    $COM = "$pgctl -l test.logfile -D $datadir stop";
    system $COM;
    sleep 3;
}

$COM = "$pgctl -l test.logfile -D $datadir start";
system $COM;
sleep 3;

pass 'Cluster has been started';

done_testing();
