
## Helper file for the pg_healer tests

use strict;
use warnings;
use Data::Dumper;
use Cwd;
use 5.006;
select(($|=1,select(STDERR),$|=1)[1]);

my $psql   = $ENV{PG_PSQL} || 'psql';
my $testport = $ENV{PGPORT} || 5555;
my $dbname = 'pg_healer_testing';
my $psql_quiet = "PGOPTIONS='--client-min-messages=error' $psql -q";
my $psql_debug = "PGOPTIONS='--client-min-messages=debug1' $psql -q";

my $testfh;
if (exists $ENV{TEST_OUTPUT}) {
	my $file = $ENV{TEST_OUTPUT};
	open $testfh, '>>', $file or die qq{Could not append file "$file": $!\n};
	Test::More->builder->failure_output($testfh);
	Test::More->builder->todo_output($testfh);
}

sub recreate_db {

    system "$psql_quiet -c 'DROP DATABASE IF EXISTS $dbname'";
    system "$psql_quiet -c 'CREATE DATABASE $dbname'";
    $psql .= " --dbname=$dbname";
    $psql_quiet .= " --dbname=$dbname";
    $psql_debug .= " --dbname=$dbname";
    return;
}

sub run {

    my $sql = shift or die "Need some SQL to run!";
    my $quiet = shift || 0;
    (my $bad_sql = $sql) =~ s/[a-z A-Z_ 0-9 \(\) ;,: '=]//g;
    if (length $bad_sql) {
        die "Keep the SQL simple, please! (invalid: $bad_sql)\n";
    }
    my $com = 1==$quiet ? $psql_quiet : 2==$quiet ? $psql_debug : $psql;
    return qx{$com -c "$sql" 2>&1};
}

sub runq {
    return run(shift, 1);
}

sub rund {
    return run(shift, 2);
}

1;
